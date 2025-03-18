#include <pthread.h>
#include <setjmp.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include "log.h"
#include "mmgr.h"
#include "defs.h"
#include "free.h"
#include "data.h"
#include "defs.h"
#include "timer.h"
#include "xlog.h"
#include "trans.h"
#include "asserts.h"

/* Log Level names */
static char* LOG_LEVEL_NAME_LIST[] = { "TRACE", "DEBUG", "INFO", "SUCCS", "WARN", "ERROR", "SYSERR", "TATAL", "PANIC" };

static char CurrentMessage[BUFF_SIZE] = {};

/* Save message to stack. */
static void save_stack_message(const char *msg) {
    bzero(CurrentMessage, BUFF_SIZE);
    memcpy(CurrentMessage, msg, min_size(BUFF_SIZE, strlen(msg)));
}


/* Get stack message. */
inline char *get_stack_message() {
    return CurrentMessage;
}

/* Get current log file desc. */
int get_current_log_fdesc() {
    char file_path[BUFF_SIZE];
    char *sys_date;

    sys_date = get_sys_time("%Y-%m-%d");
    sprintf(file_path, "%s%s.%s", conf->log_dir, sys_date, "log");
    int desc= open(file_path, O_APPEND, S_IRUSR | S_IWUSR);
    if (desc == -1) 
        db_log(
            PANIC,
            "Open table file %d fail: %s.", 
            file_path, 
            strerror(errno)
        );
    return desc;
}

/* Flush log message to disk. */
static void flush_log(char* msg) {
    char log_path[BUFF_SIZE];
    FILE *file;
    char *sys_date;

    sys_date = get_sys_time("%Y-%m-%d");
    sprintf(log_path, "%s%s.%s", conf->log_dir, sys_date, "log");
    file = fopen(log_path, "a");
    if (file == NULL) {
        fprintf(stderr, "Try to open log file '%s', error info: %s. \n", 
                log_path, strerror(errno));
        exit(1);
    }

    fputs(msg, file);
    fflush(file);
    fclose(file);
    dfree(sys_date);
}

/* Db log. */
void db_log(LogLevel level, char *format, ...) {
    Size len;
    va_list ap;

    /* Calculate the len. */
    va_start(ap, format);
    len = vsnprintf(NULL, 0, format, ap);
    if (len < 0) {
        va_end(ap);
        return;
    }

    len = len + 1;
    char message[len];
    memset(message, 0, len);

    va_start(ap, format);
    vsnprintf(message, len + 1, format, ap);
    va_end(ap);

    /* Only print higher level log. */
    if (level >= conf->log_level) {
        char *sys_time = get_current_sys_time2(MICROSECOND);
        char buff[len + 100];
        sprintf(buff, "[%s][%d][%s]:\t%s\n", 
                sys_time, getpid(), 
                LOG_LEVEL_NAME_LIST[level], message);
        fprintf(stdout, "%s", buff);
        flush_log(buff);
        dfree(sys_time);
    }

    /* According to LogLevel, there are diffent treatments. 
     * SUCCESS: statement exeucted successfully.
     * WARN:    for unexpected messages.
     * ERROR:   for expected error, abort current transaction, return to known state.
     * FATAL:   abort session.
     * PANIC:   for unexpected error, caused by system error, take down the server.
     * */
    switch(level) {
        case SUCCESS:
        case WARN: {
            save_stack_message(message);
            break;
        }
        case ERROR: {
            /* Save message to stack. */
            save_stack_message(message);
            /* Auto rollback*/
            AutoRollbackTransaction();
            /* Stop the process, goto stmt. */
            longjmp(errEnv, 1);
            break;
        }
        case SYS_ERROR: {
            /* Save message to stack. */
            save_stack_message(message);
            /* Stop the process, goto stmt. */
            // longjmp(errEnv, 1);
            break;
        }
        case FATAL:
        case PANIC: {
            /* Save message to stack. */
            save_stack_message("System inner error.");
            exit(EXIT_FAILURE);
        }
        default:
            break;
    }
}

