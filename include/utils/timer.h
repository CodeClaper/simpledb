#include "data.h"

/* Time Level. */
typedef enum { SECOND, MILLISECOND, MICROSECOND, NANOSECOND } TIME_LEVEL;

/* Get system timestamp. */
int64_t get_timestamp(TIME_LEVEL level);

/* Get system time for ms level. */
char *get_datetime(TIME_LEVEL level);

/* Get system time by format. */
char* get_sys_time(char *format);

/* Format time. */
char *format_time(char *format, time_t t);

/* Time span. */
double time_span(struct timeval end_time, struct timeval start_time);
