////
// PostgreSQL internal functions
//
#ifndef POSTGRES_FUNC_H
#define XLOG_DEFS_H

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>

// src/include/datatype/timestamp.h
#define POSTGRES_EPOCH_JDATE 2451545
#define UNIX_EPOCH_JDATE 2440588
#define SECS_PER_DAY   86400
#define USECS_PER_SEC 1000000LL

// src/include/access/xlogdefs.h
#define InvalidXLogRecPtr 0

// src/bin/pg_basebackup/streamutil.h
static
void fe_sendint64(int64_t i, char *buf)
{
    uint32_t      n32;

    /* High order half first, since we're doing MSB-first */
    n32 = (uint32_t) (i >> 32);
    n32 = htonl(n32);
    memcpy(&buf[0], &n32, 4);

    /* Now the low order half */
    n32 = (uint32_t) i;
    n32 = htonl(n32);
    memcpy(&buf[4], &n32, 4);
}

// src/bin/pg_basebackup/streamutil.h
static
int64_t fe_recvint64(char *buf)
{
    int64_t   result;
    uint32_t      h32;
    uint32_t      l32;

    memcpy(&h32, buf, 4);
    memcpy(&l32, buf + 4, 4);
    h32 = ntohl(h32);
    l32 = ntohl(l32);

    result = h32;
    result <<= 32;
    result |= l32;

    return result;
}

// src/bin/pg_basebackup/streamutil.h
static
int64_t feGetCurrentTimestamp(void)
{
    int64_t      result;
    struct timeval tp;

    gettimeofday(&tp, NULL);

    result = (int64_t) tp.tv_sec -
        ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

    result = (result * USECS_PER_SEC) + tp.tv_usec;

    return result;
}

// src/bin/pg_basebackup/streamutil.h
static
bool feTimestampDifferenceExceeds(int64_t start_time, int64_t stop_time,
        int msec)
{
    int64_t diff = stop_time - start_time;

    return (diff >= msec * 1000);
}

// src/bin/pg_basebackup/streamutil.h
static
void feTimestampDifference(int64_t start_time, int64_t stop_time,
        long *secs, int *microsecs)
{
    int64_t diff = stop_time - start_time;

    if (diff <= 0)
    {
        *secs = 0;
        *microsecs = 0;
    }
    else
    {
        *secs = (long) (diff / USECS_PER_SEC);
        *microsecs = (int) (diff % USECS_PER_SEC);
    }
}

#endif // POSTGRES_FUNC_H
