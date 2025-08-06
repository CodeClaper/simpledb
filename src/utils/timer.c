/**
* ======================================== Timer =======================================================
* The timer module supports some funcitons about time, like get_current_sys_time(TIME_LEVEL time_level).
* ======================================================================================================
*/

#include <bits/types/struct_timeval.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <math.h>
#include <stdint.h>
#include "data.h"
#include "mmgr.h"
#include "timer.h"
#include "data.h"
#include "log.h"

/* Get system timestamp.
 * Return current system timestamp, and -1 if fail.
 * Supports four time level, SECOND, MILLISECOND, MICROSECOND, NANOSECOND. */
int64_t get_timestamp(TIME_LEVEL level) {
    int64_t value = - 1;
    char time_str[32];
    struct timespec tv;

    /* Get current timestamp. */
    if (clock_gettime(CLOCK_REALTIME, &tv)) {
        db_log(PANIC, "Generate next xid error.\n");
        return -1;
    }

    switch(level) {
        case SECOND:
            value = tv.tv_sec;
        case MILLISECOND:
            sprintf(time_str, "%ld.%.3ld", tv.tv_sec, tv.tv_nsec / 1000000);
            value = round(atof(time_str) * 1e3);
            break;
        case MICROSECOND:
            sprintf(time_str, "%ld.%.6ld", tv.tv_sec, tv.tv_nsec / 1000);
            value = round(atof(time_str) * 1e6);
            break;
        case NANOSECOND:
            sprintf(time_str, "%ld.%.9ld", tv.tv_sec, tv.tv_nsec);
            value = round(atof(time_str) * 1e9);
            break;
    }

    return value;
}


/* Get system datetime for ms level. 
 * Supports four time level, SECOND, MILLISECOND, MICROSECOND, NANOSECOND. */
char *get_datetime(TIME_LEVEL level) {
    struct timeval tv;
    time_t t;
    struct tm *ptm;

    gettimeofday(&tv, NULL);
    t = tv.tv_sec;

    ptm = localtime(&t);

    char *res = dalloc(30);

    switch (level) {
        case SECOND:
            sprintf(res, "%04d-%02d-%02d %02d:%02d:%02d",
                   ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday,
                   ptm->tm_hour, ptm->tm_min, ptm->tm_sec);
            break;
        case MILLISECOND:
            sprintf(res, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                   ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday,
                   ptm->tm_hour, ptm->tm_min, ptm->tm_sec, ((int)(tv.tv_usec)) / 1000);
            break;
        case MICROSECOND:
            sprintf(res, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                   ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday,
                   ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (int)(tv.tv_usec));
            break;
        default:
            UNEXPECTED_VALUE(level);
            break;
    }


    return res;
}


/* Get system time by format. */
char* get_sys_time(char *format) {
    time_t t_now;
    time(&t_now);
    return format_time(format, t_now);
}


/* Get format time string. */
char *format_time(char *format, time_t t) {
    char *res = dalloc(20);
    struct tm *tm = localtime(&t);
    strftime(res, 20, format, tm);
    return res;
}

/* Time span. */
double time_span(struct timeval end_time, struct timeval start_time) {
    return ((end_time.tv_sec - start_time.tv_sec) * 1000000u + end_time.tv_usec - start_time.tv_usec) / 1000000.0;;
}
