#include "postgres_func.h"

#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <time.h>
#include <sys/select.h>
#include <signal.h>
#include <libpq-fe.h>
#include <getopt.h>

#define SQLSTATE_ERRCODE_OBJECT_IN_USE "55006"
#define SQLSTATE_ERRCODE_UNDEFINED_OBJECT "42704"
#define SQLSTATE_ERRCODE_DUPLICATE_OBJECT "42710"

#define OUT_BUFSIZ (32*1024)
#define CMD_BUFSIZ (4096)

struct ConfigParams {
    int count;
    const char** keys;
    const char** values;
};

struct QueryBuffer {
    char* str;
    size_t len;
    size_t bufsiz;
};

static volatile sig_atomic_t sig_abort_req = false;

static int cfg_cmd_fd = STDIN_FILENO;
static int cfg_out_fd = STDOUT_FILENO;
static int s_cmd_fd_set_flags = 0;
static FILE* s_out_file = NULL;

static bool cfg_verbose = false;
static const char* cfg_slot_name = NULL;
static struct ConfigParams cfg_pq_params;

static bool cfg_create_slot = false;
static const char* cfg_create_slot_plugin = "test_decoding";
static struct ConfigParams cfg_plugin_params;

static bool cfg_poll_mode = false;
static bool cfg_poll_has_duration = false;
static long cfg_poll_duration = 0;
static long cfg_poll_interval = 1000;

static bool cfg_write_header = false;
static bool cfg_write_nl = false;
static bool cfg_auto_feedback = false;

static long cfg_standby_message_interval = 5000;
static long cfg_feedback_interval = 0;

static char* s_cmdbuf = NULL;
static size_t s_cmdbf_len = 0;

typedef enum {
    ECODE_SUCCESS        = 0,
    ECODE_INVALID_ARGS   = 1,
    ECODE_INIT_FAILED    = 2,
    ECODE_PG_CLOSED      = 3,
    ECODE_CMD_CLOSED     = 4,
    ECODE_PG_ERROR       = 5,
    ECODE_CMD_ERROR      = 6,
    ECODE_SYSTEM_ERROR   = 7,
    ECODE_SLOT_NOT_EXIST = 8,
    ECODE_SLOT_IN_USE    = 9,
} ExitCode;

static void initConfigParam(struct ConfigParams* params)
{
    params->keys = realloc(params->keys, sizeof(char*));
    params->values = realloc(params->values, sizeof(char*));
    params->keys[0] = NULL;
    params->values[0] = NULL;
    params->count = 0;
}

static void addConfigParam(struct ConfigParams* params, const char* key, const char* value)
{
    size_t len = sizeof(char*) * (params-> count + 2);
    params->keys = realloc(params->keys, sizeof(char*) * len);
    params->values = realloc(params->values, sizeof(char*) * len);
    params->keys[params->count] = key;
    params->values[params->count] = value;
    params->count++;
    params->keys[params->count] = NULL;
    params->values[params->count] = NULL;
}

static char* addConfigParamArg(struct ConfigParams* params, const char* key_eq_val)
{
    char* arg = strdup(key_eq_val);
    char* eq = strchr(arg, '=');

    char* value;
    if (eq != NULL) {
        *eq = '\0';
        value = eq + 1;
    }
    else {
        value = NULL;
    }

    addConfigParam(params, arg, value);

    return value;
}

static void initQueryBuffer(struct QueryBuffer* qb)
{
    qb->str = malloc(512);
    qb->len = 0;
    qb->bufsiz = 512;
    qb->str[0] = '\0';
}

static void destroyQueryBuffer(struct QueryBuffer* qb)
{
    free(qb->str);
}

static void appendQueryBuffer(struct QueryBuffer* qb, const char* str)
{
    int len = strlen(str);
    if (qb->len + len + 1 < qb->bufsiz) {
        size_t new_size = qb->bufsiz * 2;
        qb->str = realloc(qb->str, new_size);
        qb->bufsiz = new_size;
    }
    memcpy(qb->str + qb->len, str, len + 1);  // copy the last '\0' byte
    qb->len += len;
}

static void sigintHandler(int signum)
{
    sig_abort_req = true;
}

static void setupSignalHandlers(void)
{
    signal(SIGINT, sigintHandler);
}

static int writeRow(
        int64_t wal_pos, int64_t wal_end, int64_t send_time,
        const char* data, size_t size)
{
    int r;

    if (cfg_write_header) {
        r = fprintf(s_out_file, "w %X/%X %lu\n",
                (uint32_t) (wal_pos >> 32), (uint32_t) wal_pos,
                size + (cfg_write_nl ? 1 : 0));
        if (r < 0) {
            return -1;
        }
    }

    r = fwrite(data, 1, size, s_out_file);
    if (r < size) {
        return -1;
    }

    if (cfg_write_nl) {
        r = fputc('\n', s_out_file);
        if (r == EOF) {
            return -1;
        }
    }

    return 0;
}

static int flushOut()
{
    int r = fflush(s_out_file);
    if (r == EOF) {
        return -1;
    }
    return 0;
}

static int processRow(char* copybuf, int buflen,
        bool* r_feedback_requested, int64_t* r_received_lsn, int64_t* r_next_feedback_lsn)
{
    if (copybuf[0] == 'k') {
        // Primary keepalive message (B)
        //   Byte1('k'), Int64, Int64, Byte1
        if (buflen < 1 + 8 + 8 + 1) {
            // Protocol error
            fprintf(stderr, "streaming header too small: %d\n", buflen);
            return -1;
        }
        int64_t wal_pos = fe_recvint64(&copybuf[1]);        // Int64 walEnd
        //int64_t send_time = fe_recvint64(&copybuf[1 + 8]);  // Int64 sendTime
        bool reply_requested = copybuf[1 + 8 + 8];          // Byte1 replyRequested
        if (reply_requested) {
            *r_feedback_requested = true;
        }
        if (*r_next_feedback_lsn == InvalidXLogRecPtr) {
            // Sending feedback can't happen with InvalidXLogRecPtr but keepalive
            // message is done by a feedback message.
            // Here needs to update next_feedback_lsn so that keepalive message
            // can be sent even when next_feedback_lsn is not set ever yet.
            *r_next_feedback_lsn = wal_pos;
        }
        // OK
        return 0;
    }
    else if (copybuf[0] == 'w') {
        // XLogData (B)
        //   Byte1('w'), Int64, Int64, Int64, ByteN
        if (buflen < 1 + 8 + 8 + 8) {
            // Protocol error
            fprintf(stderr, "streaming header too small: %d\n", buflen);
            return -1;
        }
        int64_t wal_pos = fe_recvint64(&copybuf[1]);            // Int64 dataStart
        int64_t wal_end = fe_recvint64(&copybuf[1 + 8]);        // Int64 walEnd
        int64_t send_time = fe_recvint64(&copybuf[1 + 8 + 8]);  // Int64 sendTime
        char* data = copybuf + (1 + 8 + 8 + 8);
        size_t size = buflen - (1 + 8 + 8 + 8);
        int r = writeRow(wal_pos, wal_end, send_time, data, size);
        if (r < 0) {
            // Failed to write output
            perror("failed to write data to output");
            return -2;
        }
        if (cfg_auto_feedback && *r_next_feedback_lsn < wal_end) {
            *r_next_feedback_lsn = wal_end;
        }
        if (*r_received_lsn < wal_pos) {
            *r_received_lsn = wal_pos;
        }
        // OK
        return 2;
    }
    else {
        // Protocol error
        fprintf(stderr, "unrecognized streaming header '%c', size=%d bytes\n",
                copybuf[0], buflen);
        return -1;
    }
}

static int getCmdData(void)
{
    if (s_cmd_fd_set_flags) {
        // set non-blocking flag if necessary
        if (fcntl(cfg_cmd_fd, F_SETFL, s_cmd_fd_set_flags) < 0) {
            return -1;
        }
    }

    int retval;
    int len = read(cfg_cmd_fd, s_cmdbuf + s_cmdbf_len, CMD_BUFSIZ - s_cmdbf_len);
    if (len < 0) {
        if (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK) {
            retval = 0;  // not ready to read
        }
        else {
            retval = -1;  // read error
        }
        goto done;
    }
    else if (len == 0) {
        retval = -2;  // closed
        goto done;
    }
    else {
        s_cmdbf_len += len;
        retval = len;  // OK
        goto done;
    }

done:
    if (s_cmd_fd_set_flags) {
        // Restore flags back
        if (fcntl(cfg_cmd_fd, F_SETFL, s_cmd_fd_set_flags | ~O_NONBLOCK) < 0) {
            return -1;
        }
    }
    return retval;
}

static int processOneCommand(const char* cmd, size_t len,
        int64_t* r_next_feedback_lsn, bool* r_quit_requested)
{
    if (len == 0 || cmd[0] == '#') {
        // NOP
        return 0;
    }
    else if (cmd[0] == 'F') {
        uint32_t high32;
        uint32_t low32;
        int r = sscanf(cmd, "F %X/%X", &high32, &low32);
        if (r != 2) {
            fprintf(stderr, "Invalid F command: %s\n", cmd);
            return -1;
        }
        *r_next_feedback_lsn = (((int64_t) high32) << 32) | ((int64_t) low32);
        return 0;
    }
    else if (cmd[0] == 'q') {
        *r_quit_requested = true;
        return 0;
    }
    fprintf(stderr, "Invalid command: %s\n", cmd);
    return -1;
}

static int processCommands(int64_t* r_next_feedback_lsn, bool* r_quit_requested)
{
    size_t pos = 0;

    while (true) {
        // find the next \n character from the pos
        char* next_cmd_begin = s_cmdbuf + pos;
        char* next_cmd_end = memchr(next_cmd_begin, '\n', s_cmdbf_len - pos);
        if (next_cmd_end == NULL) {
            break;
        }
        *next_cmd_end = '\0';
        size_t next_cmd_len = next_cmd_end - next_cmd_begin;

        int r = processOneCommand(next_cmd_begin, next_cmd_len,
                r_next_feedback_lsn, r_quit_requested);
        if (r < 0) {
            return -1;
        }
        pos += next_cmd_len + 1;
    }

    // bytes from 0 to pos are consumed. Move data from the pos
    // to remove consumed data.
    memmove(s_cmdbuf, s_cmdbuf + pos, s_cmdbf_len - pos);
    s_cmdbf_len -= pos;

    return 0;
}

static int sendFeedback(PGconn* conn, int64_t now, int64_t received_lsn, int64_t next_feedback_lsn)
{
    if (received_lsn < next_feedback_lsn) {
        received_lsn = next_feedback_lsn;
    }

    if (cfg_verbose) {
        fprintf(stderr, "Sending feedback: write_LSN=%X/%X flush_LSN=%X/%X\n",
                (uint32_t) (received_lsn >> 32),
                (uint32_t) received_lsn,
                (uint32_t) (next_feedback_lsn >> 32),
                (uint32_t) next_feedback_lsn);
    }

    // Standby status update (F)
    //   Byte1('r'), Int64, Int64, Int64, Int64, Byte1
    char replybuf[1 + 8 + 8 + 8 + 8 + 1];
    char* p = replybuf;
    *p = 'r';                            // 'r'
    p += 1;
    fe_sendint64(received_lsn, p);       // Int64 writeLSN
    p += 8;
    fe_sendint64(next_feedback_lsn, p);  // Int64 flushLSN
    p += 8;
    fe_sendint64(InvalidXLogRecPtr, p);  // Int64 applyLSN
    p += 8;
    fe_sendint64(now, p);                // Int64 sendTime
    p += 8;
    *p = 0;                              // Byte1 replyRequested

    if (PQputCopyData(conn, replybuf, sizeof(replybuf)) <= 0 || PQflush(conn)) {
        fprintf(stderr, "Failed to send a standby status update: %s\n", PQerrorMessage(conn));
        return -1;
    }

    return 0;
}

static long feTimestampDifferenceMillis(int64_t start_time, int64_t stop_time)
{
    long sec;
    int usec;
    feTimestampDifference(start_time, stop_time, &sec, &usec);
    return sec * 1000L + usec / 1000;
}

static bool isFeedbackNeeded(int64_t now, bool feedback_requested,
        int64_t next_feedback_lsn, int64_t last_sent_feedback_lsn,
        int64_t last_feedback_sent_at)
{
    if (next_feedback_lsn == InvalidXLogRecPtr) {
        // Feedback can't be sent with InvalidXLogRecPtr.
        return false;
    }
    return feedback_requested ||  // send feedback if server requests reply with 'k' message
        (
            // send feedback every feedback interval if next_feedback_lsn is updated
            next_feedback_lsn != last_sent_feedback_lsn &&
            feTimestampDifferenceExceeds(last_feedback_sent_at, now, cfg_feedback_interval)
        ) || (
            // send feedback every standby message interval regardless of next_feedback_lsn
            cfg_standby_message_interval != 0 &&
            feTimestampDifferenceExceeds(last_feedback_sent_at, now, cfg_standby_message_interval)
        );
}

static long selectTimeoutMillis(int64_t now,
        int64_t next_feedback_lsn, int64_t last_sent_feedback_lsn,
        int64_t last_feedback_sent_at)
{
    long minMsec = LONG_MAX;

    // send feedback every feedback interval if next_feedback_lsn is updated
    if (next_feedback_lsn != InvalidXLogRecPtr &&
            next_feedback_lsn != last_sent_feedback_lsn) {
        long msec = cfg_feedback_interval - feTimestampDifferenceMillis(last_feedback_sent_at, now);
        if (msec < minMsec) minMsec = msec;
    }

    // send feedback every standby message interval regardless of next_feedback_lsn
    if (next_feedback_lsn != InvalidXLogRecPtr &&
            cfg_standby_message_interval != 0) {
        long msec = cfg_standby_message_interval - feTimestampDifferenceMillis(last_feedback_sent_at, now);
        if (msec < minMsec) minMsec = msec;
    }

    // wait at least 300 milliseconds
    if (minMsec < 0L) {
        return 300L;
    }

    // wait at most 60 seconds
    if (minMsec > 60 * 1000L) {
        return 60 * 1000L;
    }

    return minMsec;
}

static ExitCode runLoop(PGconn* conn)
{
    ExitCode ecode;
    int64_t last_feedback_sent_at = 0;
    int64_t last_sent_feedback_lsn = InvalidXLogRecPtr;
    int64_t next_feedback_lsn = InvalidXLogRecPtr;
    int64_t received_lsn = InvalidXLogRecPtr;
    bool quit_requested = false;
    bool feedback_requested = false;
    char* copybuf = NULL;
    bool pq_ready = false;
    bool cmd_ready = false;

    while (true) {
        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        int64_t now = feGetCurrentTimestamp();

        // If feedback is needed, send feedback to PostgreSQL
        if (isFeedbackNeeded(now, feedback_requested, next_feedback_lsn, last_sent_feedback_lsn, last_feedback_sent_at)) {
            int r = sendFeedback(conn, now, received_lsn, next_feedback_lsn);
            if (r < 0) {
                ecode = ECODE_PG_ERROR;
                goto error;
            }
            last_feedback_sent_at = now;
            last_sent_feedback_lsn = next_feedback_lsn;
            feedback_requested = false;
        }

        // If abort is requested by signal, exit
        if (sig_abort_req) {
            if (cfg_verbose) {
                fprintf(stderr, "Signal received to exit.\n");
            }
            ecode = ECODE_SUCCESS;
            goto error;
        }

        // If quit is requested by a command, exit
        if (quit_requested) {
            if (cfg_verbose) {
                fprintf(stderr, "Quit command received to exit.\n");
            }
            ecode = ECODE_SUCCESS;
            goto error;
        }

        // If PQgetCopyData is ready to call, try to receive a row
        if (pq_ready) {
            if (PQconsumeInput(conn) == 0) {
                fprintf(stderr, "Failed to receive additional replication data: %s\n", PQerrorMessage(conn));
                ecode = ECODE_PG_ERROR;
                goto error;
            }
            // call select(2) only when PQgetCopyData doesn't return 0 after
            // PQconsumeInput. pq_ready is set to true if PQgetCopyData
            // returns 0.
            pq_ready = false;

            while (true) {
                // PQgetCopyData with async=true mode receives a complete row
                // and return byte size > 0. Otherwise return 0 immediately.
                int buflen = PQgetCopyData(conn, &copybuf, true);
                if (buflen > 0) {
                    int r = processRow(copybuf, buflen,
                            &feedback_requested, &received_lsn, &next_feedback_lsn);
                    if (r == -1) {
                        // Protocol error
                        ecode = ECODE_PG_ERROR;
                        goto error;
                    }
                    else if (r == -2) {
                        // Failed to write output
                        ecode = ECODE_SYSTEM_ERROR;
                        goto error;
                    }
                    pq_ready = true;
                    // continoue to PQgetCopyData call again. Call PQgetCopyData until
                    // it returns 0, then call PQconsumeInput.
                }
                else if (buflen == 0) {
                    // end pq_ready
                    break;
                }
                else if (buflen == -1) {
                    fprintf(stderr, "Replication stream closed.\n");
                    ecode = ECODE_PG_CLOSED;
                    goto error;
                }
                else {  // buflen < -1
                    fprintf(stderr, "Failed to receive replication data: %s\n", PQerrorMessage(conn));
                    ecode = ECODE_PG_ERROR;
                    goto error;
                }
            }
        }

        // If cmd is ready to receive, try to receive commands
        if (cmd_ready) {
            // getCmdData reads data from cfg_cmd_fd and
            // return byte size > 0. Otherwise return 0 immediately.
            int buflen = getCmdData();
            if (buflen > 0) {
                int r = processCommands(&next_feedback_lsn, &quit_requested);
                if (r < 0) {
                    ecode = ECODE_CMD_ERROR;
                    goto error;
                }
                if (quit_requested) {
                    // Send feedback before quit
                    feedback_requested = true;
                }
                // end cmd_ready
            }
            else if (buflen == 0) {
                cmd_ready = false;
            }
            else if (buflen == -2) {
                fprintf(stderr, "STDIN closed.\n");
                ecode = ECODE_CMD_CLOSED;
                goto error;
            }
            else {  // buflen < 0
                perror("Failed to read STDIN");
                ecode = ECODE_CMD_ERROR;
                goto error;
            }
        }

        // If pq_ready=false (last PQgetCopyData call returned 0)
        // or cmd_ready=false (last getCmdData call returned 0),
        // then use select() to wait for additional data.
        if (!pq_ready && !cmd_ready && !feedback_requested) {
            // out-of-bound flush before blocking operation
            if (flushOut() < 0) {
                perror("failed to write data to output");
                ecode = ECODE_SYSTEM_ERROR;
                goto error;
            }

            fd_set select_fds;
            int pq_socket = PQsocket(conn);
            if (pq_socket < 0) {
                fprintf(stderr, "Failed to get a socket of the connection: %s\n", PQerrorMessage(conn));
                ecode = ECODE_PG_ERROR;
                goto error;
            }

            FD_ZERO(&select_fds);
            FD_SET(pq_socket, &select_fds);
            FD_SET(cfg_cmd_fd, &select_fds);

            int max_fd = pq_socket;
            if (max_fd < cfg_cmd_fd) max_fd = cfg_cmd_fd;

            struct timeval timeout;
            long timeoutMillis = selectTimeoutMillis(now,
                    next_feedback_lsn, last_sent_feedback_lsn, last_feedback_sent_at);
            timeout.tv_sec = timeoutMillis / 1000L;
            timeout.tv_usec = timeoutMillis % 1000L * 1000L;

            int r = select(max_fd + 1, &select_fds, NULL, NULL, &timeout);
            if (r == 0 || (r < 0 && errno == EINTR)) {
                // Timeout or interrupted by a signal. Continue the loop.
            }
            else if (r < 0) {
                perror("select(2)");
                ecode = ECODE_SYSTEM_ERROR;
                goto error;
            }
            else {
                // If pq_socket is ready, call PQconsumeInput and set pq_ready=true
                if (FD_ISSET(pq_socket, &select_fds)) {
                    if (PQconsumeInput(conn) == 0) {
                        fprintf(stderr, "Failed to receive additional replication data: %s\n", PQerrorMessage(conn));
                        ecode = ECODE_PG_ERROR;
                        goto error;
                    }
                    pq_ready = true;
                }

                // If cfg_cmd_fd is ready, set cmd_ready=true
                if (FD_ISSET(cfg_cmd_fd, &select_fds)) {
                    cmd_ready = true;
                }
            }
        }

    }  // while (true)

error:
    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }

    flushOut();

    return ecode;
}

static int setNonBlocking(void)
{
    // Remove non-blocking flag from STDOUT
    int out_flags = fcntl(cfg_out_fd, F_GETFL, 0);
    if (out_flags < 0) {
        return -1;
    }
    if (fcntl(cfg_out_fd, F_SETFL, out_flags & ~O_NONBLOCK) < 0) {
        return -1;
    }

    // Set non-blocking flag to STDIN
    int in_flags = fcntl(cfg_cmd_fd, F_GETFL, 0);
    if (in_flags < 0) {
        return -1;
    }
    if (fcntl(cfg_cmd_fd, F_SETFL, in_flags | O_NONBLOCK) < 0) {
        return -1;
    }

    // Get flags of STDOUT again
    out_flags = fcntl(cfg_out_fd, F_GETFL, 0);
    if (out_flags < 0) {
        return -1;
    }

    if (out_flags & O_NONBLOCK) {
        // Setting non-blocking flag to STDIN sets non-blocking flag
        // also to STDOUT. This happens when they share the same
        // socket or tty on some platforms (darwin). In this case,
        // call fcntl always around read.
        if (fcntl(cfg_out_fd, F_SETFL, out_flags & ~O_NONBLOCK) < 0) {
            return -1;
        }
        s_cmd_fd_set_flags = in_flags | O_NONBLOCK;
    }
    else {
        s_cmd_fd_set_flags = 0;
    }

    return 0;
}

////
// > IDENTIFY_SYSTEM
//
static int runIdentifySystem(PGconn* conn)
{
    // Setup signal handlers
    setupSignalHandlers();

    // Run IDENTIFY_SYSTEM
    if (cfg_verbose) {
        fprintf(stderr, "> IDENTIFY_SYSTEM\n");
    }
    PGresult* res = PQexec(conn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "IDENTIFY_SYSTEM: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }

    // Print information
    if (cfg_verbose) {
        fprintf(stderr, "System status:\n");
        for (int r = 0; r < PQntuples(res); r++) {
            int fields = PQnfields(res);
            for (int c = 0; c < fields; c++) {
                fprintf(stderr, "  %s=%s\n", PQfname(res, c), PQgetvalue(res, r, c));
            }
        }
        fprintf(stderr, "  libpq=%d\n", PQlibVersion());
    }

    PQclear(res);
    return 0;
}

////
// > CREATE_REPLICATION_SLOT
//
static int createReplicationSlot(PGconn* conn)
{
    struct QueryBuffer qb;
    initQueryBuffer(&qb);

    if (cfg_poll_mode) {
        char* liter_slot_name = PQescapeLiteral(conn, cfg_slot_name, strlen(cfg_slot_name));
        char* liter_create_slot_plugin = PQescapeLiteral(conn, cfg_create_slot_plugin, strlen(cfg_create_slot_plugin));
        appendQueryBuffer(&qb, "select * from pg_create_logical_replication_slot(");
        appendQueryBuffer(&qb, liter_slot_name);
        appendQueryBuffer(&qb, ", ");
        appendQueryBuffer(&qb, liter_create_slot_plugin);
        appendQueryBuffer(&qb, ")");
        PQfreemem(liter_slot_name);
        PQfreemem(liter_create_slot_plugin);
    }
    else {
        char* ident_slot_name = PQescapeIdentifier(conn, cfg_slot_name, strlen(cfg_slot_name));
        char* ident_create_slot_plugin = PQescapeIdentifier(conn, cfg_create_slot_plugin, strlen(cfg_create_slot_plugin));
        appendQueryBuffer(&qb, "CREATE_REPLICATION_SLOT ");
        appendQueryBuffer(&qb, ident_slot_name);
        appendQueryBuffer(&qb, " LOGICAL ");
        appendQueryBuffer(&qb, ident_create_slot_plugin);
        PQfreemem(ident_slot_name);
        PQfreemem(ident_create_slot_plugin);
    }

    if (cfg_verbose) {
        fprintf(stderr, "> %s\n", qb.str);
    }

    PGresult* res = PQexec(conn, qb.str);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

        // If slot already exists, return 1.
        if (strcmp(SQLSTATE_ERRCODE_DUPLICATE_OBJECT, sqlstate) == 0) {
            destroyQueryBuffer(&qb);
            PQclear(res);
            return 1;
        }

        // Other errors return -1.
        fprintf(stderr, "Failed to create a replication slot (%s): %s\n",
                sqlstate, PQerrorMessage(conn));
        destroyQueryBuffer(&qb);
        PQclear(res);
        return -1;
    }

    destroyQueryBuffer(&qb);
    PQclear(res);
    return 0;
}

////
// > START_REPLICATION
//
static ExitCode runStartReplication(PGconn* conn, int64_t start_lsn)
{
    char start_lsn_buffer[16*2+1+1];
    sprintf(start_lsn_buffer, "%X/%X",
            (uint32_t) (start_lsn >> 32), (uint32_t) start_lsn);

    struct QueryBuffer qb;
    initQueryBuffer(&qb);

    {
        char* ident_slot_name = PQescapeIdentifier(conn, cfg_slot_name, strlen(cfg_slot_name));
        appendQueryBuffer(&qb, "START_REPLICATION SLOT ");
        appendQueryBuffer(&qb, ident_slot_name);
        appendQueryBuffer(&qb, " LOGICAL ");
        appendQueryBuffer(&qb, start_lsn_buffer);
        PQfreemem(ident_slot_name);
    }

    if (cfg_plugin_params.count > 0) {
        appendQueryBuffer(&qb, " (");
        for (int i=0; i < cfg_plugin_params.count; i++) {
            if (i != 0) {
                appendQueryBuffer(&qb, ", ");
            }
            if (cfg_plugin_params.values[i] != NULL) {
                char* ident_key = PQescapeIdentifier(conn, cfg_plugin_params.keys[i], strlen(cfg_plugin_params.keys[i]));
                char* liter_value = PQescapeLiteral(conn, cfg_plugin_params.values[i], strlen(cfg_plugin_params.values[i]));
                appendQueryBuffer(&qb, ident_key);
                appendQueryBuffer(&qb, " ");
                appendQueryBuffer(&qb, liter_value);
                PQfreemem(ident_key);
                PQfreemem(liter_value);
            }
            else {
                char* ident_key = PQescapeIdentifier(conn, cfg_plugin_params.keys[i], strlen(cfg_plugin_params.keys[i]));
                appendQueryBuffer(&qb, ident_key);
                PQfreemem(ident_key);
            }
        }
        appendQueryBuffer(&qb, ")");
    }

    if (cfg_verbose) {
        fprintf(stderr, "> %s\n", qb.str);
    }

    PGresult* res = PQexec(conn, qb.str);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

        // If slot is in use by another client, return ECODE_SLOT_IN_USE.
        if (strcmp(SQLSTATE_ERRCODE_OBJECT_IN_USE, sqlstate) == 0) {
            if (cfg_verbose) {
                fprintf(stderr, "Replication slot is in use: %s\n", PQerrorMessage(conn));
            }
            destroyQueryBuffer(&qb);
            PQclear(res);
            return ECODE_SLOT_IN_USE;
        }
        // If slot does not exist, return ECODE_SLOT_NOT_EXIST.
        else if (strcmp(SQLSTATE_ERRCODE_UNDEFINED_OBJECT, sqlstate) == 0) {
            if (cfg_verbose) {
                fprintf(stderr, "Replication does not exist: %s\n", PQerrorMessage(conn));
            }
            destroyQueryBuffer(&qb);
            PQclear(res);
            return ECODE_SLOT_NOT_EXIST;
        }

        // Otherwise, return ECODE_INIT_FAILED.
        fprintf(stderr, "Failed to start replication (%s): %s\n",
                sqlstate, PQerrorMessage(conn));
        destroyQueryBuffer(&qb);
        PQclear(res);
        return ECODE_INIT_FAILED;
    }

    destroyQueryBuffer(&qb);
    PQclear(res);
    return ECODE_SUCCESS;
}

static ExitCode run(void)
{
    PGconn* conn = NULL;
    ExitCode ecode;

    // Allocate input buffer
    s_cmdbuf = malloc(CMD_BUFSIZ);
    s_cmdbf_len = 0;

    // Allocate output buffer
    s_out_file = fdopen(cfg_out_fd, "a");
    setvbuf(s_out_file, NULL, _IOFBF, OUT_BUFSIZ);  // ignore errors and use default

    // Set non-blocking mode to command input file descriptor
    if (setNonBlocking() < 0) {
        perror("Invalid STDIN file descriptor");
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    // Establish the connection
    conn = PQconnectdbParams(cfg_pq_params.keys, cfg_pq_params.values, 1);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    // Run IDENTIFY_SYSTEM
    if (runIdentifySystem(conn) < 0) {
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    // Run START_REPLICATION
    ecode = runStartReplication(conn, InvalidXLogRecPtr);
    if (cfg_create_slot && ecode == ECODE_SLOT_NOT_EXIST) {
        // If slot doesn't exist and --create-slot is set, create the slot
        if (createReplicationSlot(conn) < 0) {
            ecode = ECODE_INIT_FAILED;
            goto done;
        }
        // then retry runStartReplication.
        ecode = runStartReplication(conn, InvalidXLogRecPtr);
    }
    if (ecode != ECODE_SUCCESS) {
        goto done;
    }

    // Run the main loop
    if (cfg_verbose) {
        fprintf(stderr, "Replication started\n");
    }

    ecode = runLoop(conn);

done:
    if (conn != NULL) {
        if (cfg_verbose) {
            fprintf(stderr, "Closing connection\n");
        }
        PQfinish(conn);
    }
    return ecode;
}

static ExitCode runPollLoop(PGconn* conn)
{
    ExitCode ecode;

    struct QueryBuffer qb;
    initQueryBuffer(&qb);

    {
        char* liter_slot_name = PQescapeLiteral(conn, cfg_slot_name, strlen(cfg_slot_name));
        appendQueryBuffer(&qb, "select active from pg_replication_slots where slot_name = ");
        appendQueryBuffer(&qb, liter_slot_name);
        PQfreemem(liter_slot_name);
    }

    if (cfg_verbose) {
        fprintf(stderr, "> %s\n", qb.str);
    }

    int64_t started_at = feGetCurrentTimestamp();
    while (true) {
        // Select from pg_replication_slots
        PGresult* res = PQexec(conn, qb.str);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Failed to check status of replication slot: %s\n", PQerrorMessage(conn));
            PQclear(res);
            ecode = ECODE_INIT_FAILED;
            goto done;
        }

        // Check results
        bool ready = false;
        bool exist = false;
        for (int r = 0; r < PQntuples(res); r++) {
            exist = true;
            int fields = PQnfields(res);
            for (int c = 0; c < fields; c++) {
                if (strcmp(PQfname(res, c), "active") == 0 &&
                        strcmp(PQgetvalue(res, r, c), "f") == 0) {
                    ready = true;
                }
            }
        }
        PQclear(res);

        if (ready) {
            // Slot is ready. Finish polling.
            if (cfg_verbose) {
                fprintf(stderr, "Fond the slot not in use.\n");
            }
            ecode = ECODE_SUCCESS;
            goto done;
        }
        else if (!exist && cfg_create_slot) {
            // Slot doesn't exist and --create-slot is set. Create the slot.
            if (cfg_verbose) {
                fprintf(stderr, "Slot doesn't exist.\n");
            }
            if (createReplicationSlot(conn) < 0) {
                ecode = ECODE_INIT_FAILED;
                goto done;
            }
            // Re-check status immediately
            cfg_create_slot = false;  // but don't try to create again
            continue;
        }

        // If timeout, exit.
        if (cfg_poll_has_duration) {
            int64_t now = feGetCurrentTimestamp();
            if (feTimestampDifferenceExceeds(started_at, now, cfg_poll_duration)) {
                if (exist) {
                    ecode = ECODE_SLOT_IN_USE;
                    fprintf(stderr, "Slot is in use. Timeout.\n");
                }
                else {
                    ecode = ECODE_SLOT_NOT_EXIST;
                    fprintf(stderr, "Slot doesn't exist. Timeout.\n");
                }
                goto done;
            }
        }

        // Otherwise, wait.
        if (cfg_poll_interval > 0) {
            struct timespec sp;
            sp.tv_sec = (cfg_poll_interval / 1000);
            sp.tv_nsec = (cfg_poll_interval % 1000) * 1000 * 1000;
            if (cfg_verbose) {
                if (exist) {
                    fprintf(stderr, "Slot is in use. Sleeping %.3f seconds.\n", (cfg_poll_interval / 1000.0));
                }
                else {
                    fprintf(stderr, "Slot doesn't exist. Sleeping %.3f seconds.\n", (cfg_poll_interval / 1000.0));
                }
            }
            nanosleep(&sp, NULL);
        }
    }

done:
    destroyQueryBuffer(&qb);
    return ecode;
}

static ExitCode runPoll(void)
{
    PGconn* conn = NULL;
    ExitCode ecode;

    // Establish the connection
    conn = PQconnectdbParams(cfg_pq_params.keys, cfg_pq_params.values, 1);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    ecode = runPollLoop(conn);

done:
    if (conn != NULL) {
        if (cfg_verbose) {
            fprintf(stderr, "Closing connection\n");
        }
        PQfinish(conn);
    }
    return ecode;
}

static void showUsage(void)
{
    printf("Usage: --slot=NAME [OPTION]...\n");
    printf("Options:\n");
    printf("  -?, --help                   show usage\n");
    printf("  -v, --verbose                show verbose messages\n");
    printf("  -S, --slot NAME              name of the logical replication slot\n");
    printf("  -o, --option KEY[=VALUE]     pass option NAME with optional value VALUE to the replication slot\n");
    printf("  -c, --create-slot            create a replication slot if not exist using the plugin set to --P option\n");
    printf("  -L, --poll-mode              check availability of the replication slot then exit\n");
    printf("  -D, --fd INTEGER             use the given file descriptor number instead of 1 (stdout)\n");
    printf("  -F, --feedback-interval SEC  maximum delay to send feedback to the replication slot (default: %.3f)\n", (cfg_feedback_interval / 1000.0));
    printf("  -s, --status-interval SECS   time between status messages sent to the server (default: %.3f)\n", (cfg_standby_message_interval / 1000.0));
    printf("  -A, --auto-feedback          send feedback automatically\n");
    printf("  -H, --write-header           write a header line every before a record\n");
    printf("  -N, --write-nl               write a new line character every after a record\n");
    printf("  -j, --wal2json1              equivalent to -o format-version=1 -o include-lsn=true -P wal2json\n");
    printf("  -J  --wal2json2              equivalent to -o format-version=2 --write-header -P wal2json\n");
    printf("\nCreate slot options:\n");
    printf("  -P, --plugin NAME            logical decoder plugin for a new replication slot (default: test_decoding)\n");
    printf("\nPoll mode options:\n");
    printf("  -u, --poll-duration SECS     maximum amount of time to wait until slot becomes available (default: no limit)\n");
    printf("  -i, --poll-interval SECS     interval to check availability of a slot (default: %.3f)\n", (cfg_poll_interval / 1000.0));
    printf("\nConnection options:\n");
    printf("  -d, --dbname DBNAME      database name to connect to\n");
    printf("  -h, --host HOSTNAME      database server host or socket directory\n");
    printf("  -p, --port PORT          database server port\n");
    printf("  -U, --username USERNAME  database user name\n");
    printf("  -m, --param KEY=VALUE    database connection parameter (connect_timeout, application_name, etc.)\n");
}

static int parseInterval(const char* arg, const char* arg_name, long* r_millis)
{
    char* endpos = NULL;
    double v = strtod(arg, &endpos);
    *r_millis = (long) (v * 1000);
    if (*r_millis < 0L || endpos != arg + strlen(arg)) {
        fprintf(stderr, "Invalid %s option: %s\n", arg_name, arg);
        return -1;
    }
    return 0;
}

int main(int argc, char** argv)
{
    initConfigParam(&cfg_pq_params);
    initConfigParam(&cfg_plugin_params);

    struct option longopts[] = {
        { "help",               no_argument,       NULL, '?' },
        { "verbose",            no_argument,       NULL, 'v' },
        { "slot",               required_argument, NULL, 'S' },
        { "option",             required_argument, NULL, 'o' },
        { "create-slot",        no_argument,       NULL, 'c' },
        { "poll-mode",          no_argument,       NULL, 'L' },
        { "fd",                 required_argument, NULL, 'D' },
        { "feedback-interval",  required_argument, NULL, 'F' },
        { "status-interval",    required_argument, NULL, 's' },
        { "auto-feedback",      no_argument,       NULL, 'A' },
        { "write-header",       no_argument,       NULL, 'H' },
        { "write-nl",           no_argument,       NULL, 'N' },
        { "wal2json1",          no_argument,       NULL, 'j' },
        { "wal2json2",          no_argument,       NULL, 'J' },
        { "plugin",             required_argument, NULL, 'P' },
        { "poll-duration",      required_argument, NULL, 'u' },
        { "poll-interval",      required_argument, NULL, 'i' },
        { "dbname",             required_argument, NULL, 'd' },
        { "host",               required_argument, NULL, 'h' },
        { "port",               required_argument, NULL, 'p' },
        { "username",           required_argument, NULL, 'U' },
        { "param",              required_argument, NULL, 'm' },
        { 0,                    0,                 0,     0  },
    };

    int opt;
    int longindex;
    while ((opt = getopt_long(argc, argv, "?vS:o:cLD:F:s:AHNjJP:u:i:d:h:p:U:m:", longopts, &longindex)) != -1) {
        switch (opt) {
        case '?':
            showUsage();
            return 0;
        case 'v':
            cfg_verbose = true;
            break;
        case 'S':
            cfg_slot_name = optarg;
            break;
        case 'o':
            addConfigParamArg(&cfg_plugin_params, optarg);
            break;
        case 'D':
            {
                long v = strtol(optarg, NULL, 10);
                if (v < 0L || v == STDIN_FILENO || v > INT_MAX) {
                    // Negative or STDIN is invalid
                    fprintf(stderr, "Invalid -D,--fd option: %s\n", optarg);
                    return ECODE_INVALID_ARGS;
                }
                cfg_out_fd = (int) v;
            }
            break;
        case 'c':
            cfg_create_slot = true;
            break;
        case 'P':
            cfg_create_slot_plugin = optarg;
            break;
        case 'L':
            cfg_poll_mode = true;
            break;
        case 'u':
            cfg_poll_has_duration = true;
            if (parseInterval(optarg,"-u,--poll-duration", &cfg_poll_duration) < 0) {
                return ECODE_INVALID_ARGS;
            }
            break;
        case 'i':
            if (parseInterval(optarg,"-i,--poll-interval", &cfg_poll_interval) < 0) {
                return ECODE_INVALID_ARGS;
            }
            break;
        case 'A':
            cfg_auto_feedback = true;
            break;
        case 'H':
            cfg_write_header = true;
            break;
        case 'F':
            if (parseInterval(optarg,"-F,--feedback-interval", &cfg_feedback_interval) < 0) {
                return ECODE_INVALID_ARGS;
            }
            break;
        case 's':
            if (parseInterval(optarg,"-s,--status-interval", &cfg_standby_message_interval) < 0) {
                return ECODE_INVALID_ARGS;
            }
            break;
        case 'N':
            cfg_write_nl = true;
            break;
        case 'j':
            addConfigParamArg(&cfg_plugin_params, "format-version=1");
            addConfigParamArg(&cfg_plugin_params, "include-lsn=true");
            cfg_create_slot_plugin = "wal2json";
            break;
        case 'J':
            addConfigParamArg(&cfg_plugin_params, "format-version=2");
            cfg_write_header = true;
            cfg_create_slot_plugin = "wal2json";
            break;
        case 'd':
            addConfigParam(&cfg_pq_params, "dbname", optarg);
            break;
        case 'h':
            addConfigParam(&cfg_pq_params, "host", optarg);
            break;
        case 'p':
            addConfigParam(&cfg_pq_params, "port", optarg);
            break;
        case 'U':
            addConfigParam(&cfg_pq_params, "user", optarg);
            break;
        case 'm':
            if (addConfigParamArg(&cfg_pq_params, optarg) == NULL) {
                // KEY=VALUE is required
                fprintf(stderr, "Invalid -m,--param option: %s\n", optarg);
                return ECODE_INVALID_ARGS;
            }
            break;
        default:
            printf("error! \'%c\' \'%c\'\n", opt, optopt);
            return ECODE_INVALID_ARGS;
        }
    }

    if (cfg_slot_name == NULL) {
        fprintf(stderr, "--slot NAME option must be set.\n");
        fprintf(stderr, "Use --help option to show usage.\n");
        return ECODE_INVALID_ARGS;
    }

    if (cfg_verbose) {
        fprintf(stderr, "Options:\n");
        fprintf(stderr, "  slot=%s\n", cfg_slot_name);
        fprintf(stderr, "  create-slot=%s\n", (cfg_create_slot ? "true" : "false"));
        if (cfg_create_slot) {
            fprintf(stderr, "  create-slot-plugin=%s\n", cfg_create_slot_plugin);
        }
        fprintf(stderr, "  poll-mode=%s\n", (cfg_poll_mode ? "true" : "false"));
        if (cfg_poll_mode) {
            if (cfg_poll_has_duration) {
                fprintf(stderr, "  poll-duration=%.3f\n", (cfg_poll_duration / 1000.0));
            }
            fprintf(stderr, "  poll-interval=%.3f\n", (cfg_poll_interval / 1000.0));
        }
        else {
            fprintf(stderr, "  feedback-interval=%.3f\n", (cfg_feedback_interval / 1000.0));
            fprintf(stderr, "  status-interval=%.3f\n", (cfg_standby_message_interval / 1000.0));
            fprintf(stderr, "  output-fd=%d\n", cfg_out_fd);
            fprintf(stderr, "Plugin options:\n");
            for (int i = 0; i < cfg_plugin_params.count; i++) {
                if (cfg_plugin_params.values[i] != NULL) {
                    fprintf(stderr, "  %s=%s\n", cfg_plugin_params.keys[i], cfg_plugin_params.values[i]);
                }
                else {
                    fprintf(stderr, "  %s\n", cfg_plugin_params.keys[i]);
                }
            }
        }
        fprintf(stderr, "Connection parameters:\n");
        for (int i = 0; i < cfg_pq_params.count; i++) {
            if (cfg_pq_params.values[i] != NULL) {
                fprintf(stderr, "  %s=%s\n", cfg_pq_params.keys[i], cfg_pq_params.values[i]);
            }
            else {
                fprintf(stderr, "  %s\n", cfg_pq_params.keys[i]);
            }
        }
    }

    ExitCode ecode;
    if (cfg_poll_mode) {
        ecode = runPoll();
    }
    else {
        // Setting "replication=database" establishes the connection in
        // streaming replication mode. This connection uses replication
        // protocol instead of regular SQL protocol:
        // https://www.postgresql.org/docs/current/protocol-replication.html
        addConfigParamArg(&cfg_pq_params, "replication=database");

        ecode = run();
    }

    return ecode;
}
