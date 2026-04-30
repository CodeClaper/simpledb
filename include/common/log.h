#include "data.h"

#ifndef __LOG_H__
#define __LOG_H__

#define THROW(...) (logger(PANIC, __VA_ARGS__))
#define UNREACHABLE(ret_val, ...)   \
    do {                            \
        logger(PANIC, __VA_ARGS__); \
        return(ret_val);            \
    } while (0)

char *get_log_level();
char *get_stack_message();
int get_current_log_fdesc();
void logger(LogLevel level, char *format, ...);
void logger_raw(char *format, ...);

#endif
