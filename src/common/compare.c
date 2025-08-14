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
#include <string.h>
#include <time.h>
#include "compare.h"
#include "refer.h"
#include "log.h"
#include "strheaptable.h"

/* Equal operation (=).*/
bool EQ(void *source, void *target, DataType data_type) {
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

/* Compare. */
int compare(void *source, void *taget, DataType data_type) {
    if (EQ(source, taget, data_type))
        return 0;
    else if (GT(source, taget, data_type))
        return 1;
    else 
        return -1;
}

