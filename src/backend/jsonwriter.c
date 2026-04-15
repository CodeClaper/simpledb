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
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>
#include "jsonwriter.h"
#include "data.h"
#include "mmgr.h"
#include "log.h"
#include "utils.h"
#include "meta.h"
#include "copy.h"
#include "free.h"
#include "list.h"
#include "tuple.h"
#include "trans.h"
#include "select.h"
#include "asserts.h"
#include "session.h"
#include "optimizer.h"
#include "simplify.h"
#include "strheaptable.h"

/* Handle duplicate Key. */
static void handle_dulicate_key(Row *row);
static void json_expr_node_list(List *node_list);
static void json_expr_node(ExprNode *node);

static void json_key_value_inner(Oid oid, char *key, void *value, DataType type) {
    switch(type) {
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
        case T_VARCHAR: 
            db_send("\"%s\": \"%s\"", key, value ? EscapStr((char *)value) : "null");
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
        case T_STRING: {
            char *strVal = QueryStringValue((StrRefer *)value);
            db_send("\"%s\": \"%s\"", key, strVal ? EscapStr(strVal) : "null");
            break;
        }
        /* Specially deal with T_RID data. */
        case T_RID: {
            db_send("\"%s\": ", key);
            Rid ref_id = *(Rid *)value;
            Row *subrow = DefineVisibleRow(oid, ref_id);
            json_row(subrow);
            break;
        }
        case T_OBJECT: {
            db_send("\"%s\": ", key);
            Row *subrow = (Row *)value;
            json_row(subrow);
            break;
        }
        default:
            db_log(PANIC, "Not support data type at <json_key_value>");
    }
}

static void json_key_array_value_inner(Oid oid, char *key, ArrayValue *array_value, DataType type) {
    switch (type) {
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
                char *strVal = IntToStr(value);
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
                char *strVal = LongToStr(value);
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
                char *strVal = FloatToStr(value);
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
                char *strVal = DoubleToStr(value);
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
                char *strVal = TimeToStr(value, "%Y-%m-%d %H:%M:%S");
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
                char *strVal = TimeToStr(value, "%Y-%m-%d");
                db_send("\"%s\"", strVal);
                if (last_cell(array_value->list) != lc)
                    db_send(",");
                dfree(strVal);
            }

            db_send("]");
            break;
        }
        case T_RID: {
            db_send("\"%s\": [", key);

            ListCell *lc;
            foreach (lc, array_value->list) {
                Rid ref_id = *(Rid *)lfirst(lc);
                Row *subrow = DefineVisibleRow(oid, ref_id);
                json_row(subrow);
                if (last_cell(array_value->list) != lc)
                    db_send(",");
            }

            db_send("]");
            break;
        }
        default:
            db_log(PANIC, "Not support data type at <json_key_value>");
    }
}

/* Json single-value key value. */
static void json_single_tuple_entry(MetaColumn *meta_column, void *value) {
    char *key = meta_column->column_name;
    DataType type = meta_column->column_type;
    if (value == NULL)
        db_send("\"%s\": %s", key, "null");
    else 
        json_key_value_inner(meta_column->type_oid, key, value, type);
}

/* Json array-value key value. */
static void json_array_tuple_entry(MetaColumn *meta_column, ArrayValue *array_value) {
    char *key = meta_column->column_name;
    DataType type = meta_column->column_type;
    if (!array_value)
        db_send("\"%s\": %s", key, "null");
    else 
        json_key_array_value_inner(meta_column->type_oid, key, array_value, type);
}

/* Json single-value key value. */
static void json_single_key_value(KeyValue *key_value) {
    Assert(!key_value->is_array);
    char *key = key_value->key;
    void *value = key_value->value;
    DataType type = key_value->data_type;
    if (!value)
        db_send("\"%s\": %s", key, "null");
    else {
        json_key_value_inner(key_value->tid, key, value, type);
    }
}

/* Json array-value key value. */
static void json_array_key_value(KeyValue *key_value) {
    Assert(key_value->is_array);
    char *key = key_value->key;
    ArrayValue *array_value = (ArrayValue *)key_value->value;
    DataType type = key_value->data_type;
    if (!array_value)
        db_send("\"%s\": %s", key, "null");
    else 
        json_key_array_value_inner(key_value->tid, key, array_value, type);
}

/* Json key value. */
static void json_tuple_entry(MetaColumn *meta_column, void *value) {
    if (meta_column->array_dim > 0)
        json_array_tuple_entry(meta_column, (ArrayValue *)value);
    else
        json_single_tuple_entry(meta_column, value);
}

/* Json key value. */
static void json_key_value(KeyValue *key_value) {
    Assert(key_value);
    if (key_value->is_array)
        json_array_key_value(key_value);
    else
        json_single_key_value(key_value);
}

/* Json tuple. */
void json_tuple(List *meta_columns, void *tuple) {
    if (tuple == NULL)
        db_send("null");
    else {
        db_send("{ ");
        ListCell *lc;
        foreach (lc, meta_columns) {
            MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
            if (meta_column->sys_reserved)
                continue;
            void *value = TupleFindValue(tuple, meta_column);
            json_tuple_entry(meta_column, value);
            if (last_cell(meta_columns) != lc)
                db_send(", ");
        }
        db_send(" }");
    }
}

/* Json row. */
void json_row(Row *row) {
    if (!row) db_send("null");
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

static void json_and_expr_node(ExprNode *node) {
    db_send("{");
    db_send("\"type\": \"AND\", ");
    db_send("\"left\": ");
    json_expr_node(node->leftChild);
    db_send(", ");
    db_send("\"right\": ");
    json_expr_node(node->rightChild);
    db_send(" }");
}


static void json_or_expr_node(ExprNode *node) {
    db_send("{");
    db_send("\"type\": \"OR\", ");
    db_send("\"left\": ");
    json_expr_node(node->leftChild);
    db_send(", ");
    db_send("\"right\": ");
    json_expr_node(node->rightChild);
    db_send(" }");
}

static void json_var_expr_node(ExprNode *node) {
    db_send("{\"type\": \"VAR\", \"op\": \"%s\"}", GetOprTypeName(node->opr));
}

static void json_truth_value_expr_node(ExprNode *node) {
    db_send("{\"type\": \"TRUTH_VALUE\", \"truth\": \"%s\"}", node->truthVal ? "true" : "false");
}

static void json_and_set_expr_node(ExprNode *node) {
    db_send("{");
    db_send("\"type\": \"AND_SET\", ");
    db_send("\"children\": ");
    json_expr_node_list(node->children);
    db_send(" }");
}

static void json_or_set_expr_node(ExprNode *node) {
    db_send("{");
    db_send("\"type\": \"OR_SET\", ");
    db_send("\"children\": ");
    json_expr_node_list(node->children);
    db_send(" }");
}


static void json_expr_node(ExprNode *node) {
    if (!node) db_send("null");
    else {
        switch (node->type) {
            case EXPR_AND:
                json_and_expr_node(node);
                break;
            case EXPR_OR:
                json_or_expr_node(node);
                break;
            case EXPR_VAR:
                json_var_expr_node(node);
                break;
            case EXPR_AND_SET:
                json_and_set_expr_node(node);
                break;
            case EXPR_OR_SET:
                json_or_set_expr_node(node);
                break;
            case EXPR_TRUTH_VALUE:
                json_truth_value_expr_node(node);
                break;
            case EXPR_NOT:
                break;
        }
    }
}

static void json_expr_node_list(List *node_list) {
    db_send("[");

    if (!list_null_or_empty(node_list)) {
        ListCell *lc;
        foreach (lc, node_list) {
            ExprNode *node = lfirst(lc);
            json_expr_node(node);
            if (last_cell(node_list) != lc)
                db_send(", ");
        }
    }

    db_send("]");
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
        QueueCell *qc;
        qforeach (qc, select_result->rows) {
            Row *row = qfirst(qc);
            json_row(row);
            if (QueueTail(select_result->rows) != qc)
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

/* Json login result. */
static void json_login_result(DBResult *result) {
    db_send("{ \"success\": %s,  \"message\":", result->success ? "true" : "false");
    db_send("\"%s\"", result->message);
    db_send(",\"duration\": %lf}", result->duration);
}

/* Json express result. */
static void json_express_result(DBResult *result) {
    db_send("{ \"success\": %s, \"message\": \"%s\", \"data\": ", 
            result->success ? "true" : "false", 
            result->success ? result->message : get_stack_message());
    json_expr_node(result->data);
    db_send(", \"duration\": %lf }", result->duration);
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

    if (!list_null_or_empty(list)) {
        ListCell *lc;
        foreach (lc, list) {
            List *child_list = lfirst(lc);
            json_list(child_list);
            if (last_cell(list) != lc)
                db_send(", ");
        }
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
    ListCell *lc1, *lc2;
    foreach (lc1, row->data) {
        uint32_t times = 0;
        KeyValue *first = lfirst(lc1);
        foreach (lc2, row->data) {
            KeyValue *second = lfirst(lc2);
            if (lc1 == lc2)
                continue;
            if (StrEq(second->key, first->key)) {
                second->key = FormatStr("%s(%d)", first->key, ++times);
            }
        } 
    }
}


/* Json DBResult. */
void json_db_result(DBResult *result) {
    /* If result has ouput, return. */
    if (result->hasOutput)
        return;
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
        case EXPLAIN_STMT:
            json_result_list(result);
            break;
        case EXPRESS_STMT:
            json_express_result(result);
            break;
        case LOGIN_STMT:
            json_login_result(result);
            break;
        default:
            json_nondata_result(result);
            break;
    }
}

/* Json DBResult list*/
static void json_db_result_list(List *list) {

    ListCell *lc;
    foreach (lc, list) {
        DBResult *result = lfirst(lc);
        json_db_result(result);
        if (last_cell(list) != lc)
            db_send(", ");
    }

    db_send(len_list(list) > 1 ? "]" : "");
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
