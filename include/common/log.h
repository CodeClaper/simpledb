#include "data.h"

#ifndef __LOG_H__
#define __LOG_H__

#define PANIC(...) (db_log(PANIC, __VA_ARGS__))
#define UNREACHABLE(ret_val, ...)   \
    do {                            \
        db_log(PANIC, __VA_ARGS__); \
        return(ret_val);            \
    } while (0)

char *get_stack_message();
int get_current_log_fdesc();
void db_log(LogLevel level, char *format, ...);

#endif
