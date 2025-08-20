#include <stdbool.h>
#include <stdint.h>
#include "intpr.h"
#include "data.h"
#include "free.h"
#include "log.h"
#include "mmgr.h"
#include "utils.h"

/* Paser a string value to int64_t value. */
int64_t parse_long(char *val) {
    int64_t parsed;
    ST_FLAG flag = StrToLong(val, &parsed);
    switch (flag) {
        case ST_SUCCESS:
            break;
        case ST_INVALID:
            db_log(ERROR, "'%s' is not a valid number", val);
            break;
        case ST_OVERFLOW:
            db_log(ERROR, "'%s' is overflow.", val);
            break;
        case ST_OUTRANGE:
            db_log(ERROR, "'%s' is out of range.", val);
            break;
    }
    return parsed;
}

/* Paser a string value to double value. */
double parse_double(char *val) {
    double parsed;
    ST_FLAG flag = StrToDouble(val, &parsed);
    switch (flag) {
        case ST_SUCCESS:
            break;
        case ST_INVALID:
            db_log(ERROR, "'%s' is not a valid number", val);
            break;
        case ST_OVERFLOW:
            db_log(ERROR, "'%s' is overflow.", val);
            break;
        case ST_OUTRANGE:
            db_log(ERROR, "'%s' is out of range.", val);
            break;
    }
    return parsed;
}
