#include <data.h>

/* Function name list. */
static char *FUNCITON_NAME_LIST[] = { "COUNT", "MAX", "MIN", "SUM", "AVG" };

/* Get aggregate function name. */
static inline char *GET_FUNCTION_NAME(FunctionType type) {
    return FUNCITON_NAME_LIST[type];
}

/* Check if an aggregate function. */
static inline bool IsAggFuncion(FunctionType type) {
    return type <= F_AVG;
}



