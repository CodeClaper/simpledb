/**************************************** Json output Module ******************************************
 * Auth:        JerryZhou
 * Created:     2023/12/29
 * Modify:      2024/09/13
 * Locataion:   src/backend/jsonwriter.c
 * Description: DBResult is the json format that db finally output, include flows:
 * [success]    Whether execution result is successful or unsuccessful, its value is true or false.
 * [message]    Output message to client.
 * [data]       Query data, only used when select statement.
 * [rows]       The number of rows affected.
 * [duration]   The execution time.
 ****************************************************************************************************/

#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>
#include "data.h"
#include "mmgr.h"
#include "jsonwriter.h"
#include "log.h"
#include "utils.h"
#include "meta.h"
#include "copy.h"
#include "free.h"
#include "list.h"
#include "trans.h"
#include "select.h"
#include "asserts.h"
#include "session.h"


/* Handle duplicate Key. */
static void handle_dulicate_key(Row *row);

/* Send out row. */
static void json_row(Row *row);

/* Json array-value key value. */
static void json_array_key_value(KeyValue *key_value) {
    Assert(key_value->is_array);
    char *key = key_value->key;
    ArrayValue *array_value = (ArrayValue *)key_value->value;
    if (!array_value)
        db_send("\"%s\": %s", key, "null");
    else {
        switch (key_value->data_type) {
            case T_BOOL: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    bool value = *(bool *)lfirst(lc);
                    db_send(value ? "true" : "false");
                    if (last_cell(array_value->list) != lc)
                        db_send( ",");
                }

                db_send("]");
                break;
            }
            case T_INT: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    int32_t value = *(int32_t *)lfirst(lc);
                    char *strVal = itos(value);
                    db_send(strVal);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_LONG: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    int64_t value = *(int64_t *)lfirst(lc);
                    char *strVal = ltos(value);
                    db_send(strVal);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_STRING:
            case T_VARCHAR:
            case T_CHAR: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    char *value = (char *)lfirst(lc);
                    db_send("\"%s\"", value);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                }

                db_send("]");
                break;
            }
            case T_FLOAT: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    float value = *(float *)lfirst(lc);
                    char *strVal = ftos(value);
                    db_send(strVal);
                    if (last_cell(array_value->list) != lc)
                         db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_DOUBLE: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    double value = *(double *)lfirst(lc);
                    char *strVal = dtos(value);
                    db_send(strVal);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_TIMESTAMP: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    time_t value = *(time_t *)lfirst(lc);
                    char *strVal = ttos(value, "%Y-%m-%d %H:%M:%S");
                    db_send("\"%s\"", strVal);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_DATE: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    time_t value = *(time_t *)lfirst(lc);
                    char *strVal = ttos(value, "%Y-%m-%d");
                    db_send("\"%s\"", strVal);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    dfree(strVal);
                }

                db_send("]");
                break;
            }
            case T_REFERENCE: {
                db_send("\"%s\": [", key);

                ListCell *lc;
                foreach (lc, array_value->list) {
                    Refer *refer = (Refer *)lfirst(lc);
                    Row *subrow = define_visible_row(refer);
                    json_row(subrow);
                    if (last_cell(array_value->list) != lc)
                        db_send(",");
                    free_row(subrow);
                }

                db_send("]");
                break;
            }
            default:
                db_log(PANIC, "Not support data type at <json_key_value>");
        }
    }
}

/* Json single-value key value. */
static void json_single_key_value(KeyValue *key_value) {

    Assert(!key_value->is_array);

    char *key = key_value->key;
    void *value = key_value->value;
    if (!value)
        db_send("\"%s\": %s", key, "null");
    else {
        switch(key_value->data_type) {
            case T_BOOL: 
                db_send("\"%s\": %s", key, value && (*(bool *)value) ? "true" : "false");
                break;
            case T_INT: 
                db_send("\"%s\": %d", key, value ? *(int32_t *)value : 0);
                break;
            case T_LONG: 
                db_send("\"%s\": %" PRIu64, key, value ? *(int64_t *)value : 0);
                break;
            case T_CHAR: 
            case T_STRING:
            case T_VARCHAR: 
                db_send("\"%s\": \"%s\"", key, value ? (char *)value: "null");
                break;
            case T_FLOAT: 
                db_send("\"%s\": %f", key, value ? *(float *)value : 0);
                break;
            case T_DOUBLE: 
                db_send("\"%s\": %lf", key, value ? *(double *)value : 0);
                break;
            case T_TIMESTAMP: {
                char temp[90];
                if (value) {
                    time_t t = *(time_t *)value;
                    struct tm *tmp_time = localtime(&t);
                    strftime(temp, sizeof(temp), "%Y-%m-%d %H:%M:%S", tmp_time);
                    db_send("\"%s\": \"%s\"", key, temp);
                } 
                else 
                    db_send("\"%s\": \"%s\"", key, "null");
                break;
            }
            case T_DATE: {
                char temp[90];
                if (value) {
                    time_t t = *(time_t *)value;
                    struct tm *tmp_time = localtime(&t);
                    strftime(temp, sizeof(temp), "%Y-%m-%d", tmp_time);
                    db_send("\"%s\": \"%s\"", key, temp);
                }
                else 
                    db_send("\"%s\": \"%s\"", key, "null");
                break;
            }
            /* Specially deal with T_REFERENCE data. */
            case T_REFERENCE: {
                db_send("\"%s\": ", key);
                Refer *refer = (Refer *)key_value->value;
                assert_not_null(refer, "Try to get Reference type value fail.\n");
                Row *subrow = define_visible_row(refer);
                json_row(subrow);
                free_row(subrow);
                break;
            }
            case T_ROW: {
                db_send("\"%s\": ", key);
                Row *subrow = (Row *)key_value->value;
                json_row(subrow);
                break;
            }
            default:
                db_log(PANIC, "Not support data type at <json_key_value>");
        }
    }
}

/* Json key value. */
static void json_key_value(KeyValue *key_value) {
    Assert(key_value);
    if (key_value->is_array)
        json_array_key_value(key_value);
    else
        json_single_key_value(key_value);
}

/* Json row. */
static void json_row(Row *row) {
    if (!row) 
        db_send("null");
    else {
        /* Handler duplacate key. */
        handle_dulicate_key(row);
        db_send("{ ");

        ListCell *lc;
        foreach (lc, row->data) {
            KeyValue *key_value = lfirst(lc);
            json_key_value(key_value);
            /* split with ',' */
            if (last_cell(row->data) != lc) 
                db_send(", ");
        }

        db_send(" }");
    }
}

/* Json select result. */
static void json_select_result(DBResult *result) {
    db_send("{ \"success\": %s, \"message\": \"%s\"", 
            result->success ? "true" : "false", 
            result->success ? result->message : get_stack_message());

    if (result->success) {
        db_send(", \"data\": ");
        SelectResult *select_result = result->data;
        db_send("[");
        ListCell *lc;
        foreach (lc, select_result->rows) {
            Row *row = lfirst(lc);
            json_row(row);
            if (last_cell(select_result->rows) != lc)
                db_send(", ");
        }
        db_send("]");
        db_send(", \"rows\": %d", result->rows);
    }

    db_send(", \"duration\": %lf }", result->duration);
}

/* Json result without data but rows. */
static void json_nondata_rows_result(DBResult *result) {
    db_send("{ \"success\": %s, \"message\": \"%s\", \"rows\": %d, \"duration\": %lf }", 
            result->success ? "true" : "false", 
            result->success ? result->message : get_stack_message(), result->rows, 
            result->duration);
}

/* Json result without data. */
static void json_nondata_result(DBResult *result) {
    db_send("{ \"success\": %s, \"message\": \"%s\", \"duration\": %lf }", 
            result->success ? "true" : "false", 
            result->success? result->message : get_stack_message(), 
            result->duration);
}

/* Json list of key value. */
static void json_key_value_list(List *list) {
    db_send("{ ");

    ListCell *lc;
    foreach (lc, list) {
        KeyValue *key_value = lfirst(lc);
        json_key_value(key_value);
        if (last_cell(list) != lc)
            db_send(", ");
    }

    db_send(" }");
}

/* Json list of list type. */
static void json_list_list(List *list) {
    db_send("[");

    ListCell *lc;
    foreach (lc, list) {
        List *child_list = lfirst(lc);
        json_list(child_list);
        if (last_cell(list) != lc)
            db_send(", ");
    }

    db_send("]");
}

/* Json result list. */
static void json_result_list(DBResult *result) {
    List *list = (List *)result->data;
    db_send("{ \"success\": %s, \"message\": \"%s\"", 
            result->success ? "true" : "false", 
            result->success ? result->message : get_stack_message());

    if (result->success) {
        db_send(", \"data\": ");
        json_list(list);
    }

    db_send(", \"duration\": %lf }", result->duration);
}

/* Handle duplicate Key. */
static void handle_dulicate_key(Row *row) {
    uint32_t times = 0;

    ListCell *lc1, *lc2;
    foreach (lc1, row->data) {
        KeyValue *first = lfirst(lc1);
        foreach (lc2, row->data) {
            KeyValue *second = lfirst(lc2);
            if (lc1 == lc2)
                continue;
            if (streq(second->key, first->key)) {
                dfree(second->key);
                second->key = format("%s(%d)", first->key, ++times);
            }
        } 
    }

}

/* Json DBResult. */
void json_db_result(DBResult *result) {
    switch (result->stmt_type) {
        case SELECT_STMT:
            json_select_result(result);
            break;
        case INSERT_STMT:
        case DELETE_STMT:
        case UPDATE_STMT:
            json_nondata_rows_result(result);
            break;
        case SHOW_STMT:
        case DESCRIBE_STMT:
            json_result_list(result);
            break;
        default:
            json_nondata_result(result);
            break;
    }
}

/* Json DBResult list*/
static void json_db_result_list(List *list) {
    db_send(len_list(list) > 1 ? "[" : "");

    ListCell *lc;
    foreach (lc, list) {
        DBResult *result = lfirst(lc);
        json_db_result(result);
        if (last_cell(list) != lc)
            db_send(", ");
    }

    db_send(len_list(list) > 1 ? "]\n" : "\n");
}

/* Json list. */
void json_list(List *list) {
    switch (list->type) {
        case NODE_LIST:
            json_list_list(list);
            break;
        case NODE_KEY_VALUE:
            json_key_value_list(list);
            break;
        case NODE_DB_RESULT:
            json_db_result_list(list);
            break;
        default:
            UNEXPECTED_VALUE(list->type);
            break;
    }
}

/* Banner. */
void banner() {
    DBResult result;
    result.success = true;
    result.message = "Welcome to simpledb.";
    json_db_result(&result);
}
