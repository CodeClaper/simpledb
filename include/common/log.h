#include "data.h"

#define panic(...) (db_log(PANIC, __VA_ARGS__))

char *get_stack_message();
int get_current_log_fdesc();
void db_log(LogLevel level, char *format, ...);

