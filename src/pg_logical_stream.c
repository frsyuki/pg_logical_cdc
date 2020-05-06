#include "postgres_func.h"

#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <libpq-fe.h>
#include <sys/select.h>
#include <getopt.h>

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

static bool sig_abort_req = false;

static int cfg_cmd_fd = STDIN_FILENO;
static int cfg_out_fd = STDOUT_FILENO;
static int s_cmd_fd_set_flags = 0;

static bool cfg_verbose = false;
static const char* cfg_slot_name = NULL;
static bool cfg_create_slot = false;
static struct ConfigParams cfg_plugin_params;
static struct ConfigParams cfg_pq_params;
static bool cfg_write_header = false;
static bool cfg_write_nl = false;
static bool cfg_auto_feedback = false;
static long cfg_standby_message_interval = 5000;
static long cfg_feedback_interval = 0;

static const size_t CMDBUF_SIZE = 4096;
static char* s_cmdbuf = NULL;
static size_t s_cmdbf_len = 0;

typedef enum {
    ECODE_SUCCESS       = 0,
    ECODE_INVALID_ARGS  = 1,
    ECODE_PQ_CLOSED     = 2,
    ECODE_CMD_CLOSED    = 3,
    ECODE_INIT_FAILED   = 4,
    ECODE_PQ_ERROR      = 5,
    ECODE_CMD_ERROR     = 6,
    ECODE_SYSTEM_ERROR  = 7,
} ExitCode;

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

static int writeFully(int fd, struct iovec *iov, int cnt)
{
    while (cnt > 0) {
        ssize_t r = writev(fd, iov, cnt);
        if (r < 0) {
            return -1;
        }
        while (r > 0) {
            if (iov[0].iov_len <= r) {
                r -= iov[0].iov_len;
                iov++;
                cnt--;
            }
            else {
                iov[0].iov_len -= r;
                break;
            }
        }
    }
    return 0;
}

static int writeRow(
        int64_t wal_pos, int64_t wal_end, int64_t send_time,
        const char* data, size_t size)
{
    char header_buffer[1+1 + 16+1+16+1 + 10+1 + 1];
    struct iovec iov[3];
    int cnt = 0;

    if (cfg_write_header) {
        iov[cnt].iov_base = header_buffer;
        iov[cnt].iov_len = sprintf(header_buffer, "w %X/%X %lu\n",
                (uint32_t) (wal_pos >> 32), (uint32_t) wal_pos,
                size + (cfg_write_nl ? 1 : 0));
        cnt++;
    }

    iov[cnt].iov_base = (void*) data;
    iov[cnt].iov_len = size;
    cnt++;

    if (cfg_write_nl) {
        iov[cnt].iov_base = "\n";
        iov[cnt].iov_len = 1;
        cnt++;
    }

    if (writeFully(cfg_out_fd, iov, cnt) < 0) {
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

static int getCmdData()
{
    if (s_cmd_fd_set_flags) {
        if (fcntl(cfg_cmd_fd, F_SETFL, s_cmd_fd_set_flags) < 0) {
            return -1;
        }
    }

    int retval;
    int len = read(cfg_cmd_fd, s_cmdbuf + s_cmdbf_len, CMDBUF_SIZE - s_cmdbf_len);
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
        // Restore non-flags back
        if (fcntl(cfg_cmd_fd, F_SETFL, s_cmd_fd_set_flags | ~O_NONBLOCK) < 0) {
            return -1;
        }
    }
    return retval;
}

static int processOneCommand(const char* cmd, size_t len,
        int64_t* r_next_feedback_lsn)
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
    fprintf(stderr, "Invalid command: %s\n", cmd);
    return -1;
}

static int processCommands(int64_t* r_next_feedback_lsn)
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
                r_next_feedback_lsn);
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
    bool feedback_requested = false;
    char* copybuf = NULL;
    bool pq_ready = true;  // PQgetCopyData must be called first before PQconsumeInput
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
                ecode = ECODE_PQ_ERROR;
                goto error;
            }
            last_feedback_sent_at = now;
            last_sent_feedback_lsn = next_feedback_lsn;
            feedback_requested = false;
        }

        // If abort is requested by signal, exit
        if (sig_abort_req) {
            fprintf(stderr, "Signal received to exit.\n");
            ecode = ECODE_SUCCESS;
            goto error;
        }

        // If PQgetCopyData is ready to call, try to receive a row
        if (pq_ready) {
            // PQgetCopyData with async=true mode receives a complete row
            // and return byte size > 0. Otherwise return 0 immediately.
            int buflen = PQgetCopyData(conn, &copybuf, true);
            if (buflen > 0) {
                int r = processRow(copybuf, buflen,
                        &feedback_requested, &received_lsn, &next_feedback_lsn);
                if (r == -1) {
                    // Protocol error
                    ecode = ECODE_PQ_ERROR;
                    goto error;
                }
                else if (r == -2) {
                    // Failed to write output
                    ecode = ECODE_SYSTEM_ERROR;
                    goto error;
                }
                // OK
            }
            else if (buflen == -1) {
                fprintf(stderr, "Replication stream closed.\n");
                ecode = ECODE_PQ_CLOSED;
                goto error;
            }
            else if (buflen == -2) {
                fprintf(stderr, "Failed to receive replication data: %s\n", PQerrorMessage(conn));
                ecode = ECODE_PQ_ERROR;
                goto error;
            }
            else if (buflen == 0) {
                pq_ready = false;
            }
            // do not reset pq_ready so that PQgetCopyData is called until it
            // returns 0.
        }

        // If cmd is ready to receive, try to receive commands
        if (cmd_ready) {
            // getCmdData reads data from cfg_cmd_fd and
            // return byte size > 0. Otherwise return 0 immediately.
            int buflen = getCmdData();
            if (buflen > 0) {
                int r = processCommands(&next_feedback_lsn);
                if (r < 0) {
                    ecode = ECODE_CMD_ERROR;
                    goto error;
                }
            }
            else if (buflen == -1) {
                perror("Failed to read STDIN");
                ecode = ECODE_CMD_ERROR;
                goto error;
            }
            else if (buflen == -2) {
                fprintf(stderr, "STDIN closed.\n");
                ecode = ECODE_CMD_CLOSED;
                goto error;
            }
            else if (buflen == 0) {
                cmd_ready = false;
            }
            // do not reset cmd_ready so that getCmdData is called until it
            // returns 0.
        }

        // If pq_ready=false (last PQgetCopyData call returned 0)
        // or cmd_ready=false (last getCmdData call returned 0),
        // then use select() to wait for additional data.
        if (!pq_ready && !cmd_ready && !feedback_requested) {
            fd_set select_fds;
            int pq_socket = PQsocket(conn);
            if (pq_socket < 0) {
                fprintf(stderr, "Failed to get a socket of the connection: %s\n", PQerrorMessage(conn));
                ecode = ECODE_PQ_ERROR;
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
                        ecode = ECODE_PQ_ERROR;
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

    return ecode;
}

static int setNonBlocking()
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
        // socket or tty. In this case, call fcntl always around read.
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
// > START_REPLICATION
//
static int runStartReplication(PGconn* conn, int64_t start_lsn)
{
    char start_lsn_buffer[16*2+1+1];
    sprintf(start_lsn_buffer, "%X/%X",
            (uint32_t) (start_lsn >> 32), (uint32_t) start_lsn);

    struct QueryBuffer qb;
    initQueryBuffer(&qb);

    appendQueryBuffer(&qb, "START_REPLICATION SLOT \"");
    appendQueryBuffer(&qb, cfg_slot_name);
    appendQueryBuffer(&qb, "\" LOGICAL ");
    appendQueryBuffer(&qb, start_lsn_buffer);

    if (cfg_plugin_params.count > 0) {
        appendQueryBuffer(&qb, " (");
        for (int i=0; i < cfg_plugin_params.count; i++) {
            if (i != 0) {
                appendQueryBuffer(&qb, ", ");
            }
            if (cfg_plugin_params.values[i] != NULL) {
                appendQueryBuffer(&qb, "\"");
                appendQueryBuffer(&qb, cfg_plugin_params.keys[i]);
                appendQueryBuffer(&qb, "\" '");
                appendQueryBuffer(&qb, cfg_plugin_params.values[i]);
                appendQueryBuffer(&qb, "'");
            }
            else {
                appendQueryBuffer(&qb, "\"");
                appendQueryBuffer(&qb, cfg_plugin_params.keys[i]);
                appendQueryBuffer(&qb, "\"");
            }
        }
        appendQueryBuffer(&qb, ")");
    }

    if (cfg_verbose) {
        fprintf(stderr, "> %s\n", qb.str);
    }
    PGresult* res = PQexec(conn, qb.str);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        fprintf(stderr, "Failed to start replication: %s\n", PQerrorMessage(conn));
        destroyQueryBuffer(&qb);
        PQclear(res);
        return -1;
    }

    destroyQueryBuffer(&qb);
    PQclear(res);
    return 0;
}

static ExitCode run()
{
    PGconn* conn = NULL;
    ExitCode ecode;

    // Allocate input memory
    s_cmdbuf = malloc(CMDBUF_SIZE);
    s_cmdbf_len = 0;

    // Set non-blocking mode to command input file descriptor
    if (setNonBlocking() < 0) {
        perror("Invalid STDIN file descriptor");
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    // Establilsh the connection
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
    if (runStartReplication(conn, InvalidXLogRecPtr) < 0) {
        ecode = ECODE_INIT_FAILED;
        goto done;
    }

    // Run the main loop
    if (cfg_verbose) {
        fprintf(stderr, "Replication started\n");
    }
    ecode = runLoop(conn);

done:
    if (conn != NULL) {
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
    printf("  -D, --fd INTEGER             use the given file descriptor number instead of 1 (stdout)\n");
    printf("  -F, --feedback-interval SEC  maximum delay to send feedback to the replication slot (default: %.3f)\n", (cfg_feedback_interval / 1000.0));
    printf("  -s, --status-interval SECS   time between status messages sent to the server (default: %.3f)\n", (cfg_standby_message_interval / 1000.0));
    printf("  -A, --auto-feedback          send feedback automatically\n");
    printf("  -H, --write-header           write a header line every before a record\n");
    printf("  -N, --write-nl               write a new line character every after a record\n");
    printf("  -j, --wal2json1              equivalent to -o format-version=1 -o include-lsn=true\n");
    printf("  -J  --wal2json2              equivalent to -o format-version=2 --write-header\n");
    printf("\nConnection options:\n");
    printf("  -d, --dbname DBNAME      database name to connect to\n");
    printf("  -h, --host HOSTNAME      database server host or socket directory\n");
    printf("  -p, --port PORT          database server port\n");
    printf("  -U, --username USERNAME  database user name\n");
    printf("  -m, --param KEY=VALUE    database connection parameter (connect_timeout, application_name, etc.)\n");
}

int main(int argc, char** argv)
{
    struct option longopts[] = {
        { "help",               no_argument,       NULL, '?' },
        { "verbose",            no_argument,       NULL, 'v' },
        { "slot",               required_argument, NULL, 'S' },
        { "option",             required_argument, NULL, 'o' },
        { "fd",                 required_argument, NULL, 'D' },
        { "feedback-interval",  required_argument, NULL, 'F' },
        { "status-interval",    required_argument, NULL, 's' },
        { "auto-feedback",      no_argument,       NULL, 'A' },
        { "write-header",       no_argument,       NULL, 'H' },
        { "write-nl",           no_argument,       NULL, 'N' },
        { "wal2json1",          no_argument,       NULL, 'j' },
        { "wal2json2"      ,    no_argument,       NULL, 'J' },
        { "dbname",             required_argument, NULL, 'd' },
        { "host",               required_argument, NULL, 'h' },
        { "port",               required_argument, NULL, 'p' },
        { "username",           required_argument, NULL, 'U' },
        { "param",              required_argument, NULL, 'm' },
        { 0,                    0,                 0,     0  },
    };

    int opt;
    int longindex;
    while ((opt = getopt_long(argc, argv, "?vS:o:D:F:s:AHNjJd:h:p:U:m:", longopts, &longindex)) != -1) {
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
        case 'A':
            cfg_auto_feedback = true;
            break;
        case 'H':
            cfg_write_header = true;
            break;
        case 'F':
            {
                char* endpos = NULL;
                double v = strtod(optarg, &endpos);
                cfg_feedback_interval = (int) (v * 1000);
                if (cfg_feedback_interval < 0L || endpos != optarg + strlen(optarg)) {
                    fprintf(stderr, "Invalid -F,--feedback-interval option: %s\n", optarg);
                    return ECODE_INVALID_ARGS;
                }
            }
            break;
        case 's':
            {
                char* endpos = NULL;
                double v = strtod(optarg, &endpos);
                cfg_standby_message_interval = (int) (v * 1000);
                if (cfg_standby_message_interval < 0L || endpos != optarg + strlen(optarg)) {
                    fprintf(stderr, "Invalid -s,--status-interval option: %s\n", optarg);
                    return ECODE_INVALID_ARGS;
                }
            }
            break;
        case 'N':
            cfg_write_nl = true;
            break;
        case 'j':
            addConfigParamArg(&cfg_plugin_params, "format-version=1");
            addConfigParamArg(&cfg_plugin_params, "include-lsn=true");
            break;
        case 'J':
            addConfigParamArg(&cfg_plugin_params, "format-version=2");
            cfg_write_header = true;
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
        fprintf(stderr, "  create-slot=%s\n", cfg_create_slot ? "true" : "false");
        fprintf(stderr, "  feedback-interval=%.3f\n", (cfg_feedback_interval / 1000.0));
        fprintf(stderr, "  status-interval=%.3f\n", (cfg_standby_message_interval / 1000.0));
        fprintf(stderr, "Plugin options:\n");
        for (int i = 0; i < cfg_plugin_params.count; i++) {
            if (cfg_plugin_params.values[i] != NULL) {
                fprintf(stderr, "    %s=%s\n", cfg_plugin_params.keys[i], cfg_plugin_params.values[i]);
            }
            else {
                fprintf(stderr, "    %s\n", cfg_plugin_params.keys[i]);
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

    // Setting "replication=database" establishes the connection in
    // streaming replication mode. This connection uses replication
    // protocol instead of regular SQL protocol:
    // https://www.postgresql.org/docs/current/protocol-replication.html
    addConfigParamArg(&cfg_pq_params, "replication=database");

    ExitCode ecode = run();
    return ecode;
}
