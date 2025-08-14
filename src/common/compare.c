/********************************** Compare Module **********************************************************
 * Auth:        JerryZhou
 * Created:     2023/10/07
 * Modify:      2024/11/26
 * Locataion:   src/common/compare.c
 * Description: The compare module is intended to compare two values of all kind of data type simpledb supports.
 * The comparision operations include:
 * (1) Equal.
 * (2) Not Equal.
 * (3) GT.
 * (4) GT equal.
 * (5) Less.
 * (6) Less equal.
 *************************************************************************************************************
 */
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>
#include "compare.h"
#include "refer.h"
#include "log.h"
#include "meta.h"
#include "utils.h"

static bool KeyValueEQ(KeyValue *left, KeyValue *right); 
static bool KeyValueNE(KeyValue *left, KeyValue *right); 
static bool KeyValueGT(KeyValue *left, KeyValue *right); 
static bool KeyValueGE(KeyValue *left, KeyValue *right); 
static bool KeyValueLT(KeyValue *left, KeyValue *right); 
static bool KeyValueLE(KeyValue *left, KeyValue *right); 

/* Equal operation (=).*/
bool EQ(void *source, void *target, DataType data_type) {
    /* Deal with NULL case. */
    if (source == NULL && target == NULL) 
        return true;
    else if (source != NULL && target == NULL) 
        return false;
    else if (source == NULL && target != NULL)
        return false;

    switch(data_type) {
        case T_BOOL:
            return *(bool *)source == *(bool *)target;
        case T_CHAR:
            return *(char *)source == *(char *)target;
        case T_INT:
            return *(int32_t *)source == *(int32_t *)target;
        case T_LONG:
            return *(int64_t *)source == *(int64_t *)target;
        case T_STRING: 
        case T_VARCHAR:
            return strcmp((char *)source, (char *)target) == 0;
        case T_DOUBLE:
            return *(double *)source == *(double *)target;
        case T_FLOAT:
            return *(float *)source == *(float *)target;
        case T_TIMESTAMP:
            return *(time_t *)source == *(time_t *)target;
        case T_DATE:
            return *(time_t *)source == *(time_t *)target;
        case T_REFERENCE:
            return refer_equals(source, target);
        default:
            db_log(ERROR, "Not implement data type when operate equal.");
            break;
    }

    return false;
}

/* Not equal operation. (!=)*/
bool NE(void *source, void *target, DataType data_type) {
    return !EQ(source, target, data_type);
}

/* Not equal operation. (>) */
bool GT(void *source, void *target, DataType data_type) {
    /* Deal with NULL case. */
    if (source == NULL && target == NULL) 
        return false;
    else if (source != NULL && target == NULL) 
        return true;
    else if (source == NULL && target != NULL)
        return false;

    switch(data_type) {
        case T_CHAR:
            return *(char *)source > *(char *)target;
        case T_INT:
            return *(int32_t *)source > *(int32_t *)target;
        case T_LONG:
            return *(int64_t *)source >  *(int64_t *)target;
        case T_STRING:
        case T_VARCHAR:
            return strcmp((char *)source, (char *)target) > 0;
        case T_DOUBLE:
            return *(double *)source > *(double *)target;
        case T_FLOAT:
            return *(float *)source > *(float *)target;
        case T_TIMESTAMP:
            return *(time_t *)source > *(time_t *)target;
        case T_BOOL:
            return *(bool *)source > *(bool *)target;
        case T_DATE:
            return *(time_t *)source > *(time_t *)target;
        case T_REFERENCE:
            db_log(ERROR, "Refer data not allowed to be operated GT.");
            break;
        default:
            db_log(ERROR, "Not implement data type when operate GT.");
            break;
    }

    return false;
}

/* Not equal operation. (>=) */
bool GE(void *source, void *target, DataType data_type) {
    return GT(source, target, data_type) || EQ(source, target, data_type);
}

/* Not equal operation. (<) */
bool LT(void *source, void *target, DataType data_type) {
    return !GT(source, target, data_type) && NE(source, target, data_type); 
}

/* Not equal operation. (<=) */
bool LE(void *source, void *target, DataType data_type) {
    return !GT(source, target, data_type);
}

/* Eval, now supported operation: 
 * EQ NE GT GE LT LE 
 * */
bool eval(CompareType compare_type, void *source, void *target, DataType data_type) {
    switch(compare_type) {
        case O_EQ:
            return EQ(source, target, data_type);
        case O_NE:
            return NE(source, target, data_type);
        case O_GT:
            return GT(source, target, data_type);
        case O_GE:
            return GE(source, target, data_type);
        case O_LT:
            return LT(source, target, data_type);
        case O_LE:
            return LE(source, target, data_type);
        default:
            db_log(ERROR, "Unknown compare type.");
            break;
    }
    return false;
}

/* Compare key values for EQ. */
static bool KeyValueEQ(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_BOOL: {
            switch (right->data_type) {
                case T_BOOL:
                    return EQ(left->value, right->value, T_BOOL);
                case T_STRING: {
                    bool val;
                    ST_FLAG flag = stob(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return EQ(left->value, &val, T_BOOL);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type bool.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type bool.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type bool.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_INT: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return EQ(left->value, right->value, T_INT);
                case T_STRING: {
                    int32_t val;
                    ST_FLAG flag = stoi32(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return EQ(left->value, &val, T_INT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type int.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type int.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type int.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return EQ(left->value, right->value, T_LONG);
                case T_STRING: {
                    int64_t val;
                    ST_FLAG flag = stoi64(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return EQ(left->value, &val, T_LONG);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type long.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type long.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type long.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return EQ(left->value, right->value, T_FLOAT);
                case T_STRING: {
                    float val;
                    ST_FLAG flag = stof(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return EQ(left->value, &val, T_FLOAT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type float.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return EQ(left->value, right->value, T_FLOAT);
                case T_STRING: {
                    float val;
                    ST_FLAG flag = stof(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return EQ(left->value, &val, T_FLOAT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type float.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_DATE: {
            switch (right->data_type) {
                case T_DATE:
                case T_TIMESTAMP:
                    return EQ(left->value, right->value, T_DATE);
                case T_STRING: {
                    struct tm tmp_time;
                    memset(&tmp_time, 0, sizeof(struct tm));
                    strptime(right->value, "%Y-%m-%d", &tmp_time);
                    tmp_time.tm_sec = 0;
                    tmp_time.tm_min = 0;
                    tmp_time.tm_hour = 0;
                    time_t val = mktime(&tmp_time);
                    return EQ(left->value, &val, T_DATE);
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_TIMESTAMP: {
            switch (right->data_type) {
                case T_DATE:
                case T_TIMESTAMP:
                    return EQ(left->value, right->value, T_TIMESTAMP);
                case T_STRING: {
                    struct tm tmp_time;
                    memset(&tmp_time, 0, sizeof(struct tm));
                    strptime(right->value, "%Y-%m-%d %H:%M:%S", &tmp_time);
                    time_t val = mktime(&tmp_time);
                    return EQ(left->value, &val, T_DATE);
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_CHAR: 
        case T_VARCHAR:
        case T_STRING: {
            switch (right->data_type) {
                case T_BOOL:
                case T_INT:
                case T_LONG:
                case T_DATE:
                case T_TIMESTAMP:
                    /* Same result after reversing. */
                    return KeyValueEQ(right, left);
                case T_STRING: 
                    return EQ(left->value, right->value, T_STRING);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_REFERENCE: {
            switch (right->data_type) {
                case T_REFERENCE:
                    return EQ(left->value, right->value, T_REFERENCE);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_ROW: {
            switch (right->data_type) {
                case T_ROW:
                    return EQ(left->value, right->value, T_ROW);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        default:
            UNEXPECTED_VALUE("Unknown data type.");
            break;
            
    }

ERR_TYPE:
    db_log(ERROR, "Can`t compare %s with %s", 
           GET_DATA_TYPE_NAME(left->data_type), 
           GET_DATA_TYPE_NAME(right->data_type));

    return false;
}

/* Compare key values for EQ. */
static bool KeyValueNE(KeyValue *left, KeyValue *right) {
    return !KeyValueEQ(left, right);
}

/* Compare key values for GT. */
static bool KeyValueGT(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_BOOL: {
            switch (right->data_type) {
                case T_BOOL:
                    return GT(left->value, right->value, T_BOOL);
                case T_STRING: {
                    bool val;
                    ST_FLAG flag = stob(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return GT(left->value, &val, T_BOOL);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type bool.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type bool.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type bool.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_INT: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return GT(left->value, right->value, T_INT);
                case T_STRING: {
                    int32_t val;
                    ST_FLAG flag = stoi32(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return GT(left->value, &val, T_INT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type int.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type int.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type int.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return GT(left->value, right->value, T_LONG);
                case T_STRING: {
                    int64_t val;
                    ST_FLAG flag = stoi64(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return GT(left->value, &val, T_LONG);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type long.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type long.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type long.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return GT(left->value, right->value, T_FLOAT);
                case T_STRING: {
                    float val;
                    ST_FLAG flag = stof(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return GT(left->value, &val, T_FLOAT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type float.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT:
                case T_LONG:
                    return GT(left->value, right->value, T_FLOAT);
                case T_STRING: {
                    float val;
                    ST_FLAG flag = stof(right->value, &val);
                    switch (flag) {
                        case ST_SUCCESS:
                            return GT(left->value, &val, T_FLOAT);
                        case ST_INVALID: {
                            db_log(ERROR, "Invalid input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OVERFLOW: {
                            db_log(ERROR, "Overflow input %s for type float.", right->value);
                            return false;
                        }
                        case ST_OUTRANGE: {
                            db_log(ERROR, "Out of range input %s for type float.", right->value);
                            return false;
                        }
                    }
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_DATE: {
            switch (right->data_type) {
                case T_DATE:
                case T_TIMESTAMP:
                    return GT(left->value, right->value, T_DATE);
                case T_STRING: {
                    struct tm tmp_time;
                    memset(&tmp_time, 0, sizeof(struct tm));
                    strptime(right->value, "%Y-%m-%d", &tmp_time);
                    tmp_time.tm_sec = 0;
                    tmp_time.tm_min = 0;
                    tmp_time.tm_hour = 0;
                    time_t val = mktime(&tmp_time);
                    return GT(left->value, &val, T_DATE);
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_TIMESTAMP: {
            switch (right->data_type) {
                case T_DATE:
                case T_TIMESTAMP:
                    return GT(left->value, right->value, T_TIMESTAMP);
                case T_STRING: {
                    struct tm tmp_time;
                    memset(&tmp_time, 0, sizeof(struct tm));
                    strptime(right->value, "%Y-%m-%d %H:%M:%S", &tmp_time);
                    time_t val = mktime(&tmp_time);
                    return GT(left->value, &val, T_DATE);
                }
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_CHAR: 
        case T_VARCHAR:
        case T_STRING: {
            switch (right->data_type) {
                case T_BOOL:
                case T_INT:
                case T_LONG:
                case T_DATE:
                case T_TIMESTAMP:
                    /* Same result after reversing. */
                    return KeyValueLE(right, left);
                case T_STRING: 
                    return GT(left->value, right->value, T_STRING);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_REFERENCE: {
            switch (right->data_type) {
                case T_REFERENCE:
                    return GT(left->value, right->value, T_REFERENCE);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        case T_ROW: {
            switch (right->data_type) {
                case T_ROW:
                    return GT(left->value, right->value, T_ROW);
                default:
                    goto ERR_TYPE;
            }
            break;
        }
        default:
            UNEXPECTED_VALUE("Unknown data type.");
            break;
            
    }

ERR_TYPE:
    db_log(ERROR, "Can`t compare %s with %s", 
           GET_DATA_TYPE_NAME(left->data_type), 
           GET_DATA_TYPE_NAME(right->data_type));

    return false;
}

/* Compare key values for GE. */
static bool KeyValueGE(KeyValue *left, KeyValue *right) {
    return KeyValueGT(left,right) || KeyValueEQ(left, right);
}

/* Compare key values for LT. */
static bool KeyValueLT(KeyValue *left, KeyValue *right) {
    return !KeyValueGT(left, right) && KeyValueNE(left, right);
}

/* Compare key values for LE. */
static bool KeyValueLE(KeyValue *left, KeyValue *right) {
    return !KeyValueGT(left, right);
}

/* Compare key values. */
bool KeyValueEval(CompareType compare_type, KeyValue *left, KeyValue *right) {
    switch(compare_type) {
        case O_EQ:
            return KeyValueEQ(left, right);
        case O_NE:
            return KeyValueNE(left, right);
        case O_GT:
            return KeyValueGT(left, right);
        case O_GE:
            return KeyValueGE(left, right);
        case O_LT:
            return KeyValueLT(left, right);
        case O_LE:
            return KeyValueLE(left, right);
        default:
            db_log(ERROR, "Unknown compare type.");
            break;
    }

    return false;
}

/* Compare. */
int compare(void *source, void *taget, DataType data_type) {
    if (EQ(source, taget, data_type))
        return 0;
    else if (GT(source, taget, data_type))
        return 1;
    else 
        return -1;
}

