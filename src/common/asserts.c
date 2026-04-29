/********************************** Asserts Module ********************************************************
 * Auth:        JerryZhou
 * Created:     2023/12/19
 * Modify:      2024/11/26
 * Locataion:   src/common/asserts.c
 * Description: The assert module support some common determination of whether the condition is established. 
 ***********************************************************************************************************
 */
#include <stdarg.h>
#include <stdio.h>
#include "asserts.h"
#include "data.h"
#include "log.h"

/* Assert condition is true. */
void assert_true(bool condition, char *format, ...) {
    if (!condition) {
        va_list ap;
        va_start(ap, format);
        char buff[BUFF_SIZE];
        vsprintf(buff, format, ap);
        va_end(ap);
        logger(PANIC, buff);
    }
}

/* Assert condition is false. */
void assert_false(bool condition, char *format, ...) {
    if (condition) {
        va_list ap;
        va_start(ap, format);
        char buff[BUFF_SIZE];
        vsprintf(buff, format, ap);
        va_end(ap);
        logger(PANIC, buff);
    }
}

/* Assert expression not null. */
void assert_not_null(void *express, char *format, ...) {
    if (!express) {
        va_list ap;
        va_start(ap, format);
        char buff[BUFF_SIZE];
        vsprintf(buff, format, ap);
        va_end(ap);
        logger(PANIC, buff);
    }
}

/* Assert expression not null. */
void assert_null(void *express, char *format, ...) {
    if (express) {
        va_list ap;
        va_start(ap, format);
        char buff[BUFF_SIZE];
        vsprintf(buff, format, ap);
        va_end(ap);
        logger(PANIC, buff);
    }
}
