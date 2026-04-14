#include "data.h"

#define panic(...) (db_log(PANIC, __VA_ARGS__))
#define unreachable(ret_val, ...)   \
    do {                            \
        db_log(PANIC, __VA_ARGS__); \
        return(ret_val);            \
    } while (0)

char *get_stack_message();
int get_current_log_fdesc();
void db_log(LogLevel level, char *format, ...);

