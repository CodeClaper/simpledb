/********************************** Select Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/08/13
 * Modify:      2024/11/26
 * Locataion:   src/backend/select.c
 * Description: Select modeule support select statment. 
 * Besides, Update statement, delete statement also use these module for query under conditon.
 ********************************************************************************************
 */
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>
#include "select.h"
#include "check.h"
#include "common.h"
#include "copy.h"
#include "free.h"
#include "index.h"
#include "log.h"
#include "mmgr.h"
#include "meta.h"
#include "ltree.h"
#include "pager.h"
#include "table.h"
#include "asserts.h"
#include "session.h"
#include "trans.h"
#include "refer.h"
#include "utils.h"
#include "const.h"
#include "compare.h"
#include "instance.h"
#include "jsonwriter.h"
#include "parall.h"
#include "optimizer.h"
#include "tablecache.h"
#include "strheaptable.h"

typedef struct SelectFromInternalChildTaskArgs {
    SelectResult *select_result;
    uint32_t page_num;
    uint32_t keys_num;
    ConditionNode *condition;
    Table *table;
    ROW_HANDLER row_handler;
    ROW_HANDLER_ARG_TYPE type;
    void *arg;
} SelectFromInternalChildTaskArgs;

/* Maximum number of rows fetched at once.*/
#define MAX_FETCH_ROWS 100 

/* Function name, also as key in out json. */
#define COUNT_NAME "count"
#define SUM_NAME "sum"
#define AVG_NAME "avg"
#define MAX_NAME "max"
#define MIN_NAME "min"
#define ADD_NAME "add"
#define SUB_NAME "sub"
#define MUL_NAME "mul"
#define DIV_NAME "div"

static bool include_internal_node(SelectResult *select_result, void *min_key, void *max_key, ConditionNode *condition_node, MetaTable *meta_table);
static bool include_leaf_node(SelectResult *select_result, Row *row, ConditionNode *condition_node);
static MetaColumn *get_cond_meta_column(PredicateNode *predicate, MetaTable *meta_table);
static KeyValue *query_function_value(ScalarExpNode *scalar_exp, SelectResult *select_result);
static KeyValue *query_value_item(ValueItemNode *value_item, Row *row);
static KeyValue *query_row_value(SelectResult *select_result, ScalarExpNode *scalar_exp, Row *row);
static Row *query_plain_row_selection(SelectResult *select_result, List *scalar_exp_set, Row *row);
static Row *generate_row(void *destinct, MetaTable *meta_table);
static void* purge_row(Row *row);
static char *search_table_via_alias(SelectResult *select_result, char *range_variable);
static KeyValue *query_plain_column_value(SelectResult *select_result, ColumnNode *column, Row *row);

/* Check if LimitClauseNode is full. 
 * LimitClauseNode full means the poffset is greater or equal the offset.
 * */
inline static bool limit_clause_full(SelectParam *selectParam) {
    return non_null(selectParam->limitClause) && 
        (selectParam->offset >= selectParam->limitClause->offset + selectParam->limitClause->rows);
}


/* Check if include internal comparison predicate for Value type. */
static bool include_internal_comparison_predicate_value(SelectResult *select_result, void *min_key, void *max_key, 
                                                        CompareType type, ValueItemNode *value_item, MetaColumn *meta_column) {
    bool result = false;
    void *target_key = get_value_from_value_item_node(value_item, meta_column);
    if (!target_key)
        return true;

    DataType data_type = meta_column->column_type;
    switch (type) {
        case O_EQ:
            result = less(min_key, target_key, data_type) && less_equal(target_key, max_key, data_type);
            break;
        case O_NE:
            result = !(less(min_key, target_key, data_type) && less_equal(target_key, max_key, data_type));
            break;
        case O_GT:
            result = greater(max_key, target_key, data_type);
            break;
        case O_GE:
            result = greater_equal(max_key, target_key, data_type);
            break;
        case O_LT:
            result = greater(target_key, min_key, data_type);
            break;
        case O_LE:
            result = greater(target_key, min_key, data_type);
            break;
        default:
            UNEXPECTED_VALUE("Unknown compare type.");
            break;
    }
    return result;
}

/* Check if include internal comparison predicate. */
static bool include_internal_comparison_predicate(SelectResult *select_result, void *min_key, void *max_key, 
                                                  ComparisonNode *comparison, MetaTable *meta_table) {
    ColumnNode *column = comparison->column;

    /* Other table query condition regard as true. */
    if (column->range_variable) {
        char *table_mame = search_table_via_alias(select_result, column->range_variable);
        if (select_result->last_derived && !table_mame) {
            db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
                   column->range_variable, column->column_name);
            return false;
        }
        if (!table_mame || !streq(table_mame, meta_table->table_name))
            return true;
    }

    MetaColumn *meta_column = get_meta_column_by_name(meta_table, column->column_name);
    if (meta_table == NULL) {
        db_log(ERROR, "Not found column '%s'. ", column->column_name);
        return true;
    }

    ScalarExpNode *comparsion_value = comparison->value;
    switch (comparsion_value->type) {
        case SCALAR_VALUE:
            return include_internal_comparison_predicate_value(
                select_result, 
                min_key, max_key, comparison->type, 
                comparsion_value->value, 
                meta_column
            );
        /* Other comparison value type, regarded as true 
         * for including internal predicate. */
        case SCALAR_COLUMN:
        case SCALAR_FUNCTION:
        case SCALAR_CALCULATE:
            return true;
        default:
            UNEXPECTED_VALUE("Not support comparison value type.");
            return false;
    }
}


/* Check if include the internal predicate. */
static bool include_internal_predicate(SelectResult *select_result, void *min_key, void *max_key, 
                                       PredicateNode *predicate, MetaTable *meta_table) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return include_internal_comparison_predicate(
                select_result, 
                min_key, max_key, 
                predicate->comparison, 
                meta_table
            );
        /* For in or like predicate, no skip include. */
        case PRE_IN:
        case PRE_LIKE:
            return true;
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }
}


/* Check if include the internal node if condition is logic condition. */
static bool include_logic_internal_node(SelectResult *select_result, void *min_key, void *max_key, 
                                        ConditionNode *condition_node, MetaTable *meta_table) {
    /* For logic condition node, check left node and right node. */
    switch(condition_node->conn_type) {
        case C_AND:
           return include_internal_node(select_result, min_key, max_key, condition_node->left, meta_table) && 
                        include_internal_node(select_result, max_key, max_key, condition_node->right, meta_table);
        case C_OR:
           return include_internal_node(select_result, min_key, max_key, condition_node->left, meta_table) || 
                        include_internal_node(select_result, max_key, max_key, condition_node->right, meta_table);
        case C_NONE:
            db_log(PANIC, "System logic error.");
            return false;
        default:
            UNEXPECTED_VALUE(condition_node->conn_type);
            return false;
    } 
}

/* Check if include the internal node if condition is exec condition. */
static bool include_exec_internal_node(SelectResult *select_result, void *min_key, void *max_key, 
                                       ConditionNode *condition_node, MetaTable *meta_table) {
    Assert(condition_node->conn_type == C_NONE);
    MetaColumn *cond_meta_column = get_cond_meta_column(condition_node->predicate, meta_table);

    /* Skipped the internal node must satisfy flowing factors: 
     * (1) Current condition is current table columns.
     * (2) It is primary key
     * (3) not satisfied internal node condition. */
    return !cond_meta_column || 
                !cond_meta_column->is_primary || 
                    include_internal_predicate(select_result, min_key, max_key, condition_node->predicate, meta_table);
}

/* Check if include the internal node. */
static bool include_internal_node(SelectResult *select_result, void *min_key, void *max_key, 
                                  ConditionNode *condition_node, MetaTable *meta_table) {
    /* If without condition, of course return true. */
    if (condition_node == NULL)
        return true;

    /* According to condition node type, has different way. */
    switch(condition_node->conn_type) {
        case C_OR:
        case C_AND:
            return include_logic_internal_node(
                select_result, min_key, max_key, 
                condition_node, meta_table
            );
        case C_NONE:
            return include_exec_internal_node(
                select_result, min_key, max_key, 
                condition_node, meta_table
            );
        default:
            UNEXPECTED_VALUE(condition_node->conn_type);
            return false;
    }
}

/* Check if include leaf node if the condition is logic condition. */
static bool include_logic_leaf_node(SelectResult *select_result, Row *row, ConditionNode *condition_node) {
    switch(condition_node->conn_type) {
        case C_AND:
            return include_leaf_node(select_result, row, condition_node->left) && 
                        include_leaf_node(select_result, row, condition_node->right);
        case C_OR:
            return include_leaf_node(select_result, row, condition_node->left) || 
                        include_leaf_node(select_result, row, condition_node->right);
        case C_NONE:
            db_log(PANIC, "System Logic Error");
            return false;
        default:
            UNEXPECTED_VALUE(condition_node->conn_type);
            return false;
    } 
}

/* Check the row predicate for column. */
static bool check_row_predicate_column(SelectResult *select_result, Row *row, void *value, 
                                       ColumnNode *column, CompareType type, MetaColumn *meta_column) {
    char *table_name = search_table_via_alias(select_result, column->range_variable);
    if (select_result->last_derived && table_name == NULL) {
        db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
               column->range_variable, column->column_name);
        return false;
    }
    
    /* Other table query, skip. */
    if (table_name == NULL)
        return true;

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (streq(key_value->table_name, table_name) && 
                streq(key_value->key, column->column_name)) 
            return eval(type, value, key_value->value, meta_column->column_type);
    }
    db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
           column->range_variable, 
           column->column_name);
    return false;
}

/* Check the row predicate for value. */
static bool check_row_predicate_value(SelectResult *select_result, void *value, 
                                      ValueItemNode *value_item, CompareType type, MetaColumn *meta_column) {
    void *target = get_value_from_value_item_node(value_item, meta_column);
    return eval(type, value, target, meta_column->column_type);
}

/* Check the row predicate. */
static bool check_row_predicate(SelectResult *select_result, Row *row, 
                                ColumnNode *column, ComparisonNode *comparison) {
    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (streq(key_value->key, column->column_name)) {
            /* If exists range variable, check if equals. */
            if (column->range_variable) {
                char *table_name = search_table_via_alias(select_result, column->range_variable);
                if (select_result->last_derived && table_name == NULL) {
                    db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
                           column->range_variable, column->column_name);
                    return false;
                }
                /* May subling tables, skip it temporarily. */
                if (!table_name || !streq(table_name, key_value->table_name))
                    continue;
            }

            Table *table = open_table(key_value->table_name);
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, key_value->key);
            if (meta_column == NULL) {
                db_log(ERROR, "Not found column '%s'. ", column->column_name);
                return false;
            }

            if (column->has_sub_column && column->sub_column) {
                /* Just check, if column has sub column, it must be Reference type. */
                Assert(meta_column->column_type == T_REFERENCE);
                /* Get subrow, and recursion. */
                Refer *refer = key_value->value;
                Row *sub_row = define_visible_row(refer);
                bool ret = check_row_predicate(
                    select_result, 
                    sub_row, 
                    column->sub_column, 
                    comparison
                ); 
                return ret;
            } else if (column->has_sub_column && column->scalar_exp_list) {
                db_log(ERROR, "Not support support sub column for pridicate.");
                return false;
            } else {
                ScalarExpNode *comparison_value = comparison->value;
                switch (comparison_value->type) {
                    case SCALAR_COLUMN:
                        return check_row_predicate_column(
                            select_result, row, 
                            get_real_value(key_value->value, meta_column->column_type), 
                            comparison_value->column, 
                            comparison->type, 
                            meta_column
                        );    
                    case SCALAR_VALUE: 
                        return check_row_predicate_value(
                            select_result, 
                            get_real_value(key_value->value, meta_column->column_type),
                            comparison_value->value,
                            comparison->type, 
                            meta_column
                        );
                    case SCALAR_FUNCTION:
                        db_log(ERROR, "Not support function as comparison value.");
                        return false;
                    case SCALAR_CALCULATE:
                        db_log(ERROR, "Not support calcuation comparison value.");
                        return false;
                    default:
                        UNEXPECTED_VALUE(comparison_value->type);
                        return false;
                }
            }
        }
    }
    /* When column skip test, 
     * it may exists in other tables when multi-table query. */
    return true;
}


/* Check if include leaf node satisfy comparison predicate. */
static bool include_leaf_comparison_predicate(SelectResult *select_result, Row *row, ComparisonNode *comparison) {
    return check_row_predicate(select_result, row, comparison->column, comparison);
}

/* Check if include in value item set. */
static bool check_in_value_item_set(List *value_list, void *value, MetaColumn *meta_column) {
    ListCell *lc;
    foreach (lc, value_list) {
        void *target = get_value_from_value_item_node(lfirst(lc), meta_column);
        if (equal(value, target, meta_column->column_type))
            return true;
    }
    return false;
}

/* Check if include leaf node satisfy in predicate. */
static bool include_leaf_in_predicate(Row *row, InNode *in_node) {
    Table *table = open_table(row->table_name);

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        /* Define the column. */
        if (streq(key_value->key, in_node->column->column_name)) {
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table , key_value->key);
            return check_in_value_item_set(
                in_node->value_list, 
                get_real_value(key_value->value, meta_column->column_type), 
                meta_column
            );
        }
    }
    return false;
}

/* Check if satisfy like string value. */
static bool check_like_string_value(char *value, char *target) {
    size_t value_len = strlen(value);
    size_t target_len = strlen(target);
    if (value_len == 0 || target_len == 0)
        return false;

    if (target[0] == '%' && target[target_len - 1] == '%') {
        char str_dup[target_len];
        memset(str_dup, 0, target_len);
        memcpy(str_dup, target + 1, target_len -2);
        return contains(value, str_dup);
    }
    else if (target[0] == '%')
        return endwith(value, target + 1);
    else if (target[target_len - 1] == '%') {
        char str_dup[target_len];
        memset(str_dup, 0, target_len);
        memcpy(str_dup, target, target_len - 1);
        return startwith(value, str_dup);
    } 
    else 
        return streq(value, target);
}


/* Check if include leaf node satisfy like predicate. */
static bool include_leaf_like_predicate(Row *row, LikeNode *like_node) {
    Table *table = open_table(row->table_name);

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        /* Define the column. */
        if (streq(key_value->key, like_node->column->column_name)) {
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, key_value->key);
            void *target_value = get_value_from_value_item_node(like_node->value, meta_column);
            return check_like_string_value(get_real_value(key_value->value, meta_column->column_type), target_value);
        }
    }

    return false;
}

/* Check if include leaf node if the condition is exec condition. */
static bool include_exec_leaf_node(SelectResult *select_result, Row *row, ConditionNode *condition_node) {
    /* If without condition, of course the key include, so just return true. */
    if (condition_node == NULL)
        return true;

    Assert(condition_node->conn_type == C_NONE);

    PredicateNode *predicate = condition_node->predicate;
    switch (predicate->type) {
        case PRE_COMPARISON:
            return include_leaf_comparison_predicate(select_result, row, predicate->comparison);
        case PRE_IN:
            return include_leaf_in_predicate(row, predicate->in);
        case PRE_LIKE:
            return include_leaf_like_predicate(row, predicate->like);
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }

}

/* Check if the key include the leaf node. */
static bool include_leaf_node(SelectResult *select_result, Row *row, ConditionNode *condition_node) {
    /* If not visible for current transaction, return false. */
    if (!RowIsVisible(row))
        return false;

    /* If without condition, of course the key include, so just return true. */
    if (condition_node == NULL) 
          return true;

    switch(condition_node->conn_type) {
        case C_OR:
        case C_AND:
            return include_logic_leaf_node(select_result, row, condition_node);
        case C_NONE:
            return include_exec_leaf_node(select_result, row, condition_node);
        default:
            UNEXPECTED_VALUE(condition_node->conn_type);
            return false;
    }
}

/* Get meta column by condition name. */
static MetaColumn *get_cond_meta_column(PredicateNode *predicate, MetaTable *meta_table) {
    if (predicate == NULL)
        return NULL;
    switch (predicate->type)  {
        case PRE_COMPARISON:
            return get_meta_column_by_name(meta_table, predicate->comparison->column->column_name);
        case PRE_LIKE:
            return get_meta_column_by_name(meta_table, predicate->like->column->column_name);
        case PRE_IN:
            return get_meta_column_by_name(meta_table, predicate->in->column->column_name);
        default:
            UNEXPECTED_VALUE(predicate->type);
            return NULL;
    }
}

/* Get row array value. 
 * Return ArrayValue.
 * */
static ArrayValue *get_row_array_value(void *destination, MetaColumn *meta_column) {
    uint32_t array_num = get_array_number(destination);

    /* Generate ArrayValue instance. */
    ArrayValue *array_value = new_array_value(meta_column->column_type, array_num);
    uint32_t span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;

    uint32_t i;
    for (i = 0; i < array_num; i++) {
        void *value = get_array_value(destination, i, span);
        append_list(array_value->list, copy_value(value, meta_column->column_type));
    }
    return array_value;
}

/* Assignment row value. */
static void *assign_row_value(void *destination, MetaColumn *meta_column) {
    return (meta_column->array_dim == 0)
            /* For non-array data. */
            ? destination + LEAF_NODE_CELL_NULL_FLAG_SIZE 
            /* For array data. */
            : get_row_array_value(destination, meta_column); 
}


/* Generate select row. */
static Row *generate_row(void *destination, MetaTable *meta_table) {
    /* Instance new row. */
    Row *row = new_row(NULL, meta_table->table_name);

    /* Assignment row data. */
    uint32_t i, offset = 0;
    for (i = 0; i < meta_table->all_column_size; i++) {
        MetaColumn *meta_column = meta_table->meta_column[i];
        /* Generate a key value pair. */
        KeyValue *key_value = is_null_cell(destination + offset) 
                            ? new_key_value(meta_column->column_name, NULL, meta_column->column_type)
                            : new_key_value(meta_column->column_name, assign_row_value(destination + offset, meta_column), meta_column->column_type);
        key_value->is_array = meta_column->array_dim > 0;
        key_value->table_name = meta_table->table_name;

        /* Append to row data. */
        append_list(row->data, key_value);

        /* Assign primary key. */
        if (meta_column->is_primary)
            row->key = assign_row_value(destination + offset, meta_column);

        /* Get the column offset. */
        offset += meta_column->column_length;
    }

    return row;
}

/* Define row by refer. 
 * -------------------
 * Return row not matter if it is deleted, caller check the row if deleted.
 * The row is raw, can`t be freed by caller.
 * */
Row *define_row(Refer *refer) {
    Assert(refer != NULL);

    /* Check table exists. */
    Table *table = open_table_inner(refer->oid);
    if (table == NULL)
        return NULL;

    /* Check if refer null. */
    if (refer_null(refer))
        return NULL;

    uint32_t key_len, value_len;
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);

    /* Get the leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), refer->page_num);
    LockBuffer(buffer, RW_READERS);
    void *leaf_node = GetBufferPage(buffer);

    void *destinct = get_leaf_node_cell_value(leaf_node, key_len, value_len, refer->cell_num);
    Row *row = generate_row(destinct, table->meta_table);
    
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    return row;
}

/* Define row by refer. 
 * Return undelted, filtered row, return NULL if deleted.
 * */
Row *define_visible_row(Refer *refer) {
    Row *row = define_row(refer);
    return (RowIsDeleted(row))
        ? NULL
        : purge_row(row);
}

/* Merge the second row data into the first one. */
static void merge_row(Row *row1, Row *row2) {
    /* According the first row generate new merge row. */
    ListCell *lc;
    foreach (lc, row2->data) {
        append_list(row1->data, lfirst(lc));
    }
}

/* Merge two SelectResult. */
static void merge_select_result(SelectResult *result1, SelectResult *result2) {
    QueueCell *qc;
    qforeach (qc, result2->rows) {
        AppendQueue(result1->rows, qfirst(qc));
    }
    result1->row_size += result2->row_size;
}

/* Search table via alias name in SelectResult. 
 * Note: range variable may be table name or table alias name.
 * */
static char *search_table_via_alias(SelectResult *select_result, char *range_variable) {
    if (select_result == NULL) 
        db_log(PANIC, "Support SelectResult. ");

    /* Either table name or range variable is equal. */
    if (streq(select_result->table_name, range_variable) || 
            streq(select_result->range_variable, range_variable))
        return select_result->table_name;

    if (select_result->derived)
        return search_table_via_alias(select_result->derived, range_variable);

    return NULL;
}

/* Select through leaf node. */
static void select_from_leaf_node(SelectResult *select_result, ConditionNode *condition, 
                                  uint32_t page_num, Table *table, ROW_HANDLER row_handler, 
                                  ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* Get cell number, key length and value lenght. */
    uint32_t key_len, value_len, cell_num ;
    Buffer buffer;
    void *leaf_node;

    /* If LimitClauseNode full, not continue. */
    if (type == ARG_SELECT_PARAM && limit_clause_full(arg))
        return;

    /* Get leaf node buffer. */
    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);

    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    cell_num = get_leaf_node_cell_num(leaf_node, value_len);

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        /* Get leaf node cell value. */
        void *destinct = get_leaf_node_cell_value(leaf_node, key_len, value_len, i);

        /* If satisfied, exeucte row handler function. */
        Row *row = generate_row(destinct, table->meta_table);

        SelectResult *derived = select_result->derived;
        if (derived != NULL) {
            /* Cartesian product. */
            QueueCell *qc;
            qforeach (qc, derived->rows) {
                /* Merge derived-row. */
                Row *derived_row = qfirst(qc);
                merge_row(derived_row, row);
                free_common_row(row);

                /* Check if the row data include,in another word, 
                 * check if the row data satisfy the condition. */
                if (include_leaf_node(select_result, derived_row, condition)) 
                    row_handler(derived_row, select_result, table, type, arg);
                else
                    free_common_row(derived_row);
            }
            continue;
        }

        /* Check if the row data include. In another word, 
         * check if the row data satisfy the condition. */
        if (include_leaf_node(select_result, row, condition)) 
            row_handler(row, select_result, table, type, arg);
        else
            free_common_row(row);
    }

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Select through internal node. */
static void select_from_internal_node(SelectResult *select_result, ConditionNode *condition, 
                                      uint32_t page_num, Table *table, 
                                      ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* If LimitClauseNode full, not continue. */
    if (type == ARG_SELECT_PARAM && limit_clause_full(arg))
        return;

    /* Get the internal node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    void *internal_node = GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);

    /* Get variables. */
    uint32_t key_len, value_len, keys_num;
    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    keys_num = get_internal_node_keys_num(internal_node, value_len);

    DataType primary_key_type = get_primary_key_type(table->meta_table);

    /* Loop each interanl node cell to check if satisfy condition. 
     * Note that: get the internal node keys number in each loop.
     * It`s important for reading when inserting in the concurrency scenario.
     * */
    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        /* Check if index column, use index to avoid full text scanning. */
        {
            /* Current internal node cell key as max key, previous cell key as min key. */
            void *max_key = get_real_value(get_internal_node_key(internal_node, i, key_len, value_len), primary_key_type); 
            void *min_key = (i == 0) 
                        ? NULL 
                        : get_real_value(get_internal_node_key(internal_node, i - 1, key_len, value_len), primary_key_type);
            if (!include_internal_node(select_result, min_key, max_key, condition, table->meta_table))
                continue;
        }

        /* Check other non-key column */
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
        Assert(child_page_num != 0);
        Buffer child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
        LockBuffer(child_buffer, RW_READERS);
        void *node = GetBufferPageCopy(child_buffer);
        UnlockBuffer(child_buffer);

        switch (get_node_type(node)) {
            case LEAF_NODE:
                select_from_leaf_node(
                    select_result, condition, child_page_num, 
                    table, row_handler, type, arg
                );
                break;
            case INTERNAL_NODE:
                select_from_internal_node(
                    select_result, condition, child_page_num, 
                    table, row_handler, type, arg
                );
                break;
            default:
                db_log(PANIC, "Unknown node type.");
                break;
        }
        
        free_block(node);
        /* Release the child buffer. */
        ReleaseBuffer(child_buffer);
    }

    /* Don`t forget the right child. */
    /* Fetch right child. */
    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
    Buffer right_child_buffer = ReadBuffer(GET_TABLE_OID(table), right_child_page_num);
    LockBuffer(right_child_buffer, RW_READERS);
    void *right_child = GetBufferPageCopy(right_child_buffer);
    UnlockBuffer(right_child_buffer);
    NodeType node_type = get_node_type(right_child);
    switch (node_type) {
        case LEAF_NODE:
            select_from_leaf_node(
                select_result, 
                condition, right_child_page_num, 
                table, row_handler, type, arg
            );
            break;
        case INTERNAL_NODE:
            select_from_internal_node(
                select_result, 
                condition, right_child_page_num, 
                table, row_handler, type, arg
            );
            break;
        default:
            UNEXPECTED_VALUE(node_type);
            break;
    }
 
    free_block(internal_node); 
    free_block(right_child);

    /* Release buffers. */
    ReleaseBuffer(right_child_buffer);
    ReleaseBuffer(buffer);
}


static void select_from_internal_node_child_task(void *taskArg) {
    Assert(taskArg != NULL);

    SelectFromInternalChildTaskArgs *args = (SelectFromInternalChildTaskArgs *) taskArg;
    uint32_t child_page_num = args->page_num;
    SelectResult *select_result = args->select_result;
    ConditionNode *condition = args->condition;
    Table *table = args->table;
    ROW_HANDLER row_handler = args->row_handler;
    ROW_HANDLER_ARG_TYPE type = args->type;
    void *arg = args->arg;
    Buffer child_buffer;
    void *child_node;

    child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
    LockBuffer(child_buffer, RW_READERS);
    child_node = GetBufferPageCopy(child_buffer);
    UnlockBuffer(child_buffer);

    switch (get_node_type(child_node)) {
        case LEAF_NODE:
            select_from_leaf_node(
                select_result, condition, child_page_num, 
                table, row_handler, type, arg
            );
            break;
        case INTERNAL_NODE:
            select_from_internal_node(
                select_result, condition, child_page_num, 
                table, row_handler, type, arg
            );
            break;
        default:
            db_log(PANIC, "Unknown node type.");
            break;
    }

    free_block(child_node);
    /* Release the child buffer. */
    ReleaseBuffer(child_buffer);
}

/* Select through internal node. */
static void select_from_internal_node_async(SelectResult *select_result, ConditionNode *condition, 
                                            uint32_t page_num, Table *table, 
                                            ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* If LimitClauseNode full, not continue. */
    if (non_null(arg) && limit_clause_full(arg))
        return;

    /* Get the internal node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);

    /* Get variables. */
    uint32_t key_len, value_len, keys_num;
    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    keys_num = get_internal_node_keys_num(internal_node, value_len);

    DataType primary_key_type = get_primary_key_type(table->meta_table);

    /* Prepare the parallel computing task args. */
    uint32_t taskNum = 0;
    SelectFromInternalChildTaskArgs *taskArgs[keys_num + 1];
    SelectResult *selectResults[keys_num + 1];

    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        void *max_key = get_real_value(get_internal_node_key(internal_node, i, key_len, value_len), primary_key_type); 
        void *min_key = (i == 0) 
                    ? NULL 
                    : get_real_value(get_internal_node_key(internal_node, i - 1, key_len, value_len), primary_key_type);
        if (!include_internal_node(select_result, min_key, max_key, condition, table->meta_table))
            continue;
        
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
        selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name);
        taskArgs[taskNum] = instance(SelectFromInternalChildTaskArgs);
        taskArgs[taskNum]->select_result = selectResults[taskNum];
        taskArgs[taskNum]->page_num = child_page_num;
        taskArgs[taskNum]->keys_num = keys_num;
        taskArgs[taskNum]->condition = condition;
        taskArgs[taskNum]->table = table;
        taskArgs[taskNum]->row_handler = row_handler;
        taskArgs[taskNum]->type = type;
        taskArgs[taskNum]->arg = arg;
        taskNum++;
    }
   
    /* Don`t forget the right child. */
    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
    selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name);
    taskArgs[taskNum] = instance(SelectFromInternalChildTaskArgs);
    taskArgs[taskNum]->select_result = selectResults[taskNum];
    taskArgs[taskNum]->page_num = right_child_page_num;
    taskArgs[taskNum]->keys_num = keys_num;
    taskArgs[taskNum]->condition = condition;
    taskArgs[taskNum]->table = table;
    taskArgs[taskNum]->row_handler = row_handler;
    taskArgs[taskNum]->type = type;
    taskArgs[taskNum]->arg = arg;
    taskNum++;

    /* Parallel compute. */
    ParallelCompute(8, taskNum, select_from_internal_node_child_task, (void **)taskArgs);

    /* Merge select result. */
    for (i = 0; i < taskNum; i++) {
        merge_select_result(select_result, selectResults[i]);
    }

    ReleaseBuffer(buffer);
}

/* The condition of executing async. 
 * --------------------------------
 * Must satisfy two condtions: 
 * (1) SELECT_STMT.
 * (2) Already in cache.
 * Note: why it must be in cache. Because, IO operation is slow, 
 * all threads wait for loading data from disk. The concurrency does not
 * improve the performance. On the contrary, the frequent switch of context
 * will affects the performance.
 * */
static bool async_condition(SelectResult *select_result) {
    return select_result->stype == SELECT_STMT && 
                TableNameExistsInCache(select_result->table_name);
}

/* Query with condition inner. */
void query_with_condition_inner(Oid oid, ConditionNode *condition, SelectResult *select_result, 
                          ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {

    Table *table;
    Buffer buffer;
    void *root;
    
    /* Check if table exists. */
    table = open_table_inner(oid);
    if (table == NULL)
        return;

    buffer = ReadBuffer(GET_TABLE_OID(table), table->root_page_num); 
    root = GetBufferPage(buffer);

    switch (get_node_type(root)) {
        case LEAF_NODE:
            select_from_leaf_node(
                select_result, condition, table->root_page_num, 
                table, row_handler, type, arg
            );
            break;
        case INTERNAL_NODE: {
            if (async_condition(select_result)) 
                select_from_internal_node_async(
                    select_result, condition, table->root_page_num,
                    table, row_handler, type, arg
                );
            else
                select_from_internal_node(
                    select_result, condition, table->root_page_num,
                    table, row_handler, type, arg
                );
            break;
        }
        default:
            db_log(PANIC, "Unknown data type occurs in <query_with_condition>.");
    }

    /* Release the root node buffer. */
    ReleaseBuffer(buffer);
}

/* Query with condition. */
void query_with_condition(ConditionNode *condition, SelectResult *select_result, 
                          ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Check if table exists. */
    Table *table = open_table(select_result->table_name);
    if (table == NULL) {
        db_log(ERROR, "Table %s not exist.", select_result->table_name);
        return;
    }
    query_with_condition_inner(GET_TABLE_OID(table), condition, select_result, row_handler, type, arg);
}

static ConditionNode *ColumnValueConvertCondition(MetaColumn *meta_column, void *value) {
    ConditionNode *condition = instance(ConditionNode);
    condition->conn_type = C_NONE;
    condition->left = NULL;
    condition->right = NULL;
    condition->predicate = instance(PredicateNode);
    condition->predicate->type = PRE_COMPARISON;
    condition->predicate->comparison = instance(ComparisonNode);
    condition->predicate->comparison->type = O_EQ;
    condition->predicate->comparison->column = instance(ColumnNode);
    condition->predicate->comparison->column->column_name = dstrdup(meta_column->column_name);
    condition->predicate->comparison->value = instance(ScalarExpNode);
    condition->predicate->comparison->value->type = SCALAR_VALUE;
    condition->predicate->comparison->value->value = instance(ValueItemNode);
    condition->predicate->comparison->value->value->type = V_ATOM;
    condition->predicate->comparison->value->value->value.atom = combine_atom_node(meta_column, value);
    return condition;
}


/* Query with column and value. 
 * ---------------------------
 * This function will query table with column-value condition.
 * And return the SelectResult which freed by caller. 
 * */
SelectResult *select_with_column_value(Oid oid, MetaColumn *meta_column, void *value) {
    /* Check if table exists. */
    Table *table = open_table_inner(oid);
    Assert(table != NULL);
    ConditionNode *condtion = ColumnValueConvertCondition(meta_column, value);
    SelectResult *result = new_select_result(SELECT_STMT, GET_TABLE_NAME(table));
    query_with_condition_inner(oid, condtion, result, select_row, ARG_NULL, NULL);
    return result;
}

/* Count number of row, used in the sql function count(1) */
void count_row(Row *row, SelectResult *select_result, Table *table, 
               ROW_HANDLER_ARG_TYPE type,void *arg) {

    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
            select_result->row_size++;
            select_result->rows->size++;
        }

        selectParam->offset++;
    } 
    else {
        select_result->row_size++;
        select_result->rows->size++;
    }

    /* Not use row info, free it. */
    free_common_row(row);
}

/* Purge row. 
 * Pruge means to remove the sys-reserved column.
 * */
static void* purge_row(Row *row) {
    List *list = row->data;
    /* At least, more 3 sys-reserved column. */
    Assert(list->size > 3);

    /* Delete last 3 sys-reserved items. */
    list_delete_tail(list, 3);

    return row;
}

/* Select row data. */
void select_row(Row *row, SelectResult *select_result, Table *table, 
                ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {

            AppendQueue(select_result->rows, purge_row(row));
            select_result->row_size++;
        }

        selectParam->offset++;
    } else {
        AppendQueue(select_result->rows, purge_row(row));
        select_result->row_size++;
    }
}

/* Query row data. 
 * --------------
 * Defferent with select_row, query_row will output the row 
 * data immediately and then free memory, not store the row 
 * data until selection. It works for query-rows operation.
 * */
void query_row(Row *row, SelectResult *select_result, Table *table, 
               ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
            
            json_row(purge_row(row));
            select_result->row_size++;
        }
        selectParam->offset++;
    } else {
        json_row(purge_row(row));
        select_result->row_size++;
    }
}


/* Calulate column sum value. */
static KeyValue *calc_column_sum_value(ColumnNode *column, SelectResult *select_result) {
    double sum = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = query_plain_column_value(select_result, column, row);
        switch (key_value->data_type) {
            case T_INT: {
                sum += *(int32_t *)key_value->value;
                break;
            }
            case T_LONG: {
                sum += *(int64_t *)key_value->value;
                break;
            }
            case T_FLOAT: {
                sum += *(float *)key_value->value;
                break;
            }
            case T_DOUBLE: {
                sum += *(double *)key_value->value;
                break;
            }
            case T_REFERENCE: 
            case T_ROW: {
                db_log(ERROR, "Reference type not used for sum function.");
                break;
            }
            default: {
                sum += 0;
                break;
            }
        }
    }
    return new_key_value(dstrdup(SUM_NAME), copy_value(&sum, T_DOUBLE), T_DOUBLE);
}


/* Calulate column avg value. */
static KeyValue *calc_column_avg_value(ColumnNode *column, SelectResult *select_result) {
    double sum = 0;
    double avg = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = query_plain_column_value(select_result, column, row);
        switch (key_value->data_type) {
            case T_INT: {
                sum += *(int32_t *)key_value->value;
                break;
            }
            case T_LONG: {
                sum += *(int64_t *)key_value->value;
                break;
            }
            case T_FLOAT: {
                sum += *(float *)key_value->value;
                break;
            }
            case T_DOUBLE: {
                sum += *(double *)key_value->value;
                break;
            }
            case T_REFERENCE: 
            case T_ROW: {
                db_log(ERROR, "Reference type not used for avg function.");
                break;
            }
            default: {
                sum += 0;
                break;
            }
        }
    }
    avg = sum / (select_result->rows->size);
    return new_key_value(dstrdup(AVG_NAME), copy_value(&avg, T_DOUBLE), T_DOUBLE);
}


/* Calulate column max value.*/
static KeyValue *calc_column_max_value(ColumnNode *column, SelectResult *select_result) {
    void *max_value = NULL;
    DataType data_type;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = query_plain_column_value(select_result, column, row);
        data_type = current->data_type;
        void *current_value = current->value;
        if (!max_value || greater(
                get_real_value(current_value, data_type), 
                get_real_value(max_value, data_type), 
                data_type)) 
        {
            if (max_value)
                free_value(max_value, data_type);
            max_value = copy_value(current_value, data_type);
        }
    }
    return new_key_value(dstrdup(MAX_NAME), max_value, data_type);
}

/* Calulate column max value.*/
static KeyValue *calc_column_min_value(ColumnNode *column, SelectResult *select_result) {
    void *min_value = NULL;
    DataType data_type;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = query_plain_column_value(select_result, column, row);
        data_type = current->data_type;
        void *current_value = current->value;
        if (min_value == NULL || less(
                get_real_value(current_value, data_type), 
                get_real_value(min_value, data_type), 
                data_type)) 
        {
            if (min_value)
                free_value(min_value, data_type);
            min_value = copy_value(current_value, data_type);
        }
    }
    return new_key_value(dstrdup(MIN_NAME), min_value, data_type);
}


/* Query count function. */
static KeyValue *query_count_function(FunctionValueNode *value, SelectResult *select_result) {
    uint32_t row_size = select_result->row_size;
    return new_key_value(dstrdup("count"), copy_value(&row_size, T_INT), T_INT);
}

/* Query sum function. */
static KeyValue *query_sum_function(FunctionValueNode *value, SelectResult *select_result) {
    switch (value->value_type) {
        case V_COLUMN: 
            return calc_column_sum_value(value->column, select_result);
        case V_INT: {
            double sum = value->i_value * (select_result->rows->size);
            return new_key_value(dstrdup(SUM_NAME), copy_value(&sum, T_DOUBLE), T_DOUBLE);
        }
        case V_ALL: {
            db_log(ERROR, "Sum function not support '*'");
            return NULL;
        }
        default: {
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
        }
    }
}

/* Query avg function. */
KeyValue *query_avg_function(FunctionValueNode *value, SelectResult *select_result) {

    switch (value->value_type) {
        case V_COLUMN:
            return calc_column_avg_value(value->column, select_result);
        case V_INT: 
            return new_key_value(dstrdup(AVG_NAME), copy_value(&value->i_value, T_DOUBLE), T_DOUBLE);
        case V_ALL: 
            db_log(ERROR, "Avg function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
}

/* Query max function. */
KeyValue *query_max_function(FunctionValueNode *value, SelectResult *select_result) {

    switch (value->value_type) {
        case V_COLUMN: 
            return calc_column_max_value(value->column, select_result);
        case V_INT: 
            return new_key_value(
                dstrdup(MAX_NAME), 
                copy_value(&value->i_value, T_INT), 
                T_INT
            );
        case V_ALL: 
            db_log(ERROR, "Max function not support '*'.");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 


/* Query min function. */
KeyValue *query_min_function(FunctionValueNode *value, SelectResult *select_result) {

    switch (value->value_type) {
        case V_COLUMN:
            return calc_column_min_value(value->column, select_result);
        case V_INT: 
            return new_key_value(
                dstrdup(MIN_NAME), 
                copy_value(&value->i_value, T_INT), 
                T_INT
            );
        case V_ALL: 
            db_log(PANIC, "Min function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 

/* Query scalar function */
static KeyValue *query_function_column_value(FunctionNode *function, SelectResult *select_result) {
    switch (function->type) { 
        case F_COUNT:
            return query_count_function(function->value, select_result);
        case F_SUM:
            return query_sum_function(function->value, select_result);
        case F_AVG:
            return query_avg_function(function->value, select_result);
        case F_MAX:
            return query_max_function(function->value, select_result);
        case F_MIN:
            return query_min_function(function->value, select_result);
        default:
            UNEXPECTED_VALUE("Not implement function yet.");
            return NULL;
    }
}


/* Query column value. */
static KeyValue *query_plain_column_value(SelectResult *select_result, ColumnNode *column, Row *row) {

    if (row == NULL) 
        return new_key_value(dstrdup(column->column_name), NULL, T_ROW);

    /* Get table name via alias name. */
    char *table_name = search_table_via_alias(select_result, column->range_variable);
    if (column->range_variable && table_name == NULL) {
        db_log(ERROR, "Unknown table alias '%s' in select items. ", 
               column->range_variable);
        return NULL;
    }

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (streq(column->column_name, key_value->key) && 
                (table_name == NULL || streq(table_name, key_value->table_name))) {
            /* Reference type and query sub column. */
            if (key_value->data_type == T_REFERENCE) {
                Refer *refer = (Refer *)key_value->value;
                Row *sub_row = define_visible_row(refer);
                if (column->has_sub_column && column->sub_column) {
                    KeyValue *sub_key_value = query_plain_column_value(select_result, column->sub_column, sub_row);
                    return sub_key_value;
                } else if (column->has_sub_column && column->scalar_exp_list) {
                    Row *filtered_subrow = query_plain_row_selection(select_result, column->scalar_exp_list, sub_row);
                    return new_key_value(
                        dstrdup(column->column_name), 
                        filtered_subrow, 
                        T_ROW
                    );
                } else if (!column->has_sub_column) {
                    return new_key_value(
                        dstrdup(column->column_name), 
                        sub_row, 
                        T_ROW
                    );
                }
            }
            else if (column->has_sub_column) 
                db_log(ERROR, "Column '%s' is not Reference type, no sub column found.", 
                       column->column_name);
            else
                return (key_value);
        }
    }
    db_log(ERROR, "Not found column '%s'. ", column->column_name);
    return NULL;
}

/* Calulate addition. */
static KeyValue *calulate_addition(KeyValue *left, KeyValue *right) {
    
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int32_t sum = *(int32_t *)left->value + *(int32_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_INT), 
                                         T_INT);
                }
                case T_LONG: {
                    int64_t sum = *(int32_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float sum = *(int32_t *)left->value + *(float *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sum = *(int32_t *)left->value + *(double *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&zero, T_DOUBLE), 
                                         T_DOUBLE);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sum = *(int64_t *)left->value + *(int32_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_LONG), 
                                         T_LONG);
                }
                case T_LONG: {
                    int64_t sum = *(int64_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float sum = *(int64_t *)left->value + *(float *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sum = *(int64_t *)left->value + *(double *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sum = *(float *)left->value + *(int32_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_LONG: {
                    float sum = *(float *)left->value + *(int64_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_FLOAT: {
                    float sum = *(float *)left->value + *(float *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sum = *(float *)left->value + *(double *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sum = *(double *)left->value + *(int32_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double sum = *(double *)left->value + *(int64_t *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double sum = *(double *)left->value + *(float *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double sum = *(double *)left->value + *(double *)right->value;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&sum, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(ADD_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(dstrdup(ADD_NAME), 
                                 copy_value(&zero, T_INT), 
                                 T_INT);
        }
    }
}

/* Calulate substraction .*/
static KeyValue *calulate_substraction(KeyValue *left, KeyValue *right) {
    
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int32_t sub = *(int32_t *)left->value - *(int32_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_INT), 
                                         T_INT);
                }
                case T_LONG: {
                    int64_t sub = *(int32_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float sub = *(int32_t *)left->value - *(float *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sub = *(int32_t *)left->value - *(double *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sub = *(int64_t *)left->value - *(int32_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_LONG), 
                                         T_LONG);
                }
                case T_LONG: {
                    int64_t sub = *(int64_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float sub = *(int64_t *)left->value - *(float *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sub = *(int64_t *)left->value - *(double *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sub = *(float *)left->value - *(int32_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_LONG: {
                    float sub = *(float *)left->value - *(int64_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_FLOAT: {
                    float sub = *(float *)left->value - *(float *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double sub = *(float *)left->value - *(double *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sub = *(double *)left->value - *(int32_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double sub = *(double *)left->value - *(int64_t *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double sub = *(double *)left->value - *(float *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double sub = *(double *)left->value - *(double *)right->value;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&sub, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(SUB_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(dstrdup(SUB_NAME), 
                                 copy_value(&zero, T_INT), 
                                 T_INT);
        }
    }
}


/* Calulate multiplication .*/
static KeyValue *calulate_multiplication(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t mul = (*(int32_t *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_INT), 
                                         T_INT);
                }
                case T_LONG: {
                    int64_t mul = (*(int32_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float mul = (*(int32_t *)left->value) * (*(float *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double mul = (*(int32_t *)left->value) * (*(double *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_LONG), 
                                         T_LONG);
                }
                case T_LONG: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_LONG), 
                                         T_LONG);
                }
                case T_FLOAT: {
                    float mul = (*(int64_t *)left->value) * (*(float *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double mul = (*(int64_t *)left->value) * (*(double *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_LONG), 
                                         T_LONG);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&zero, T_LONG), 
                                         T_LONG);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float mul = (*(float *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_LONG: {
                    float mul = (*(float *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_FLOAT: {
                    float mul = (*(float *)left->value) * (*(float *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_FLOAT), 
                                         T_FLOAT);
                }
                case T_DOUBLE: {
                    double mul = (*(float *)left->value) * (*(double *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double mul = (*(double *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double mul = (*(double *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double mul = (*(double *)left->value) * (*(float *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double mul = (*(double *)left->value) * (*(double *)right->value);
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&mul, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(MUL_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(dstrdup(MUL_NAME), 
                                 copy_value(&zero, T_INT), 
                                 T_INT);
        }
    }
}

/* Calulate division .*/
static KeyValue *calulate_division(KeyValue *left, KeyValue *right) {
    
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)(*(int32_t *)left->value) / (*(int32_t *)right->value);
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double div = (double)(*(int64_t *)left->value) / (*(int64_t *)right->value);
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double div = (double)(*(int32_t *)left->value) / (*(float *)right->value);
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double div = (double)(*(int32_t *)left->value) / *(double *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(int64_t *)left->value / *(int32_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double div = (double)*(int64_t *)left->value / *(int64_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double div = (double)*(int64_t *)left->value / *(float *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double div = (double)*(int64_t *)left->value / *(double *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(float *)left->value / *(int32_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double div = (double)*(float *)left->value / *(int64_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double div = (double)*(float *)left->value / *(float *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double div = (double)*(float *)left->value / *(double *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double div = *(double *)left->value / *(int32_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_LONG: {
                    double div = *(double *)left->value / *(int64_t *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_FLOAT: {
                    double div = *(double *)left->value / *(float *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                case T_DOUBLE: {
                    double div = *(double *)left->value / *(double *)right->value;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&div, T_DOUBLE), 
                                         T_DOUBLE);
                }
                default: {
                    int zero = 0;
                    return new_key_value(dstrdup(DIV_NAME), 
                                         copy_value(&zero, T_INT), 
                                         T_INT);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(dstrdup(DIV_NAME), 
                                 copy_value(&zero, T_INT), 
                                 T_INT);
        }
    }
}

/* Query function calculate. */
static KeyValue *query_function_calculate_column_value(CalculateNode *calculate, SelectResult *select_result) {
    KeyValue *result = NULL;

    KeyValue *left = query_function_value(calculate->left, select_result);
    KeyValue *right = query_function_value(calculate->right, select_result);
    
    switch (calculate->type) {
        case CAL_ADD:
            result = calulate_addition(left, right);
            break;
        case CAL_SUB:
            result = calulate_substraction(left, right);
            break;
        case CAL_MUL:
            result = calulate_multiplication(left, right);
            break;
        case CAL_DIV:
            result = calulate_division(left, right);
            break;
        default:
            UNEXPECTED_VALUE(calculate->type);
            break;
    }
    return result;
}


/* Query column value. */
static KeyValue *query_function_value(ScalarExpNode *scalar_exp, SelectResult *select_result) {
    Table *table = open_table(select_result->table_name);
    switch (scalar_exp->type) {
        case SCALAR_COLUMN: {
            ColumnNode *column = scalar_exp->column;
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, column->column_name);
            if (QueueIsEmpty(select_result->rows)) {
                return new_key_value(
                    column->column_name, 
                    NULL, 
                    meta_column->column_type
                );
            }
            else {
                /* Default, when query function and column data, 
                 * column only return first data. */
                return query_plain_column_value(
                    select_result, 
                    column, 
                    qfirst(QueueHead(select_result->rows))
                );
            }
        }
        case SCALAR_FUNCTION:
            return query_function_column_value(scalar_exp->function, select_result);
        case SCALAR_CALCULATE:
            return query_function_calculate_column_value(scalar_exp->calculate, select_result);
        case SCALAR_VALUE: {
            ValueItemNode *value = scalar_exp->value;
            if (QueueIsEmpty(select_result->rows)) 
                return new_key_value(
                    dstrdup("value"), 
                    NULL, 
                    convert_data_type(value->value.atom->type)
                );
            else
                return query_value_item(value, qfirst(QueueHead(select_result->rows)));
        }
        default: {
            UNEXPECTED_VALUE("Unknown scalar type");
            return NULL;
        }
    } 
}

/* Query function data. */
static void query_fuction_selecton(List *scalar_exp_list, SelectResult *select_result) {
    Row *row = new_row(NULL, select_result->table_name);

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = query_function_value(scalar_exp, select_result);        
        if (scalar_exp->alias) {
            // free_value(key_value->key, T_STRING);
            key_value->key = dstrdup(scalar_exp->alias);
        }
        append_list(row->data, key_value);
    }

    select_result->rows = CreateQueue(NODE_ROW);
    AppendQueue(select_result->rows, row);
}

/* Query all-columns calcuate column value. */
static KeyValue *query_all_columns_calculate_column_value(SelectResult *select_result, CalculateNode *calculate, Row *row) {
    KeyValue *result = NULL;

    KeyValue *left = query_row_value(select_result, calculate->left, row);
    KeyValue *right = query_row_value(select_result, calculate->right, row);

    switch (calculate->type) {
        case CAL_ADD:
            result = calulate_addition(left, right);
            break;
        case CAL_SUB:
            result = calulate_substraction(left, right);
            break;
        case CAL_MUL:
            result = calulate_multiplication(left, right);
            break;
        case CAL_DIV:
            result = calulate_division(left, right);
            break;
    }
    return result;
}

/* Query value item in scalar_exp. */
static KeyValue *query_value_item(ValueItemNode *value_item, Row *row) {
    Assert(value_item->type == V_ATOM);
    AtomNode *atom_node = value_item->value.atom;
    switch (atom_node->type) {
        case A_INT:
            return new_key_value(dstrdup("value"), 
                                 copy_value(&atom_node->value.intval, T_LONG), 
                                 T_LONG);
        case A_BOOL:
            return new_key_value(dstrdup("value"), 
                                 copy_value(&atom_node->value.boolval, T_BOOL), 
                                 T_BOOL);
        case A_FLOAT:
            return new_key_value(dstrdup("value"), 
                                 copy_value(&atom_node->value.boolval, T_DOUBLE), 
                                 T_DOUBLE);
        case A_STRING:
            return new_key_value(dstrdup("value"), 
                                 copy_value(atom_node->value.strval, T_STRING), 
                                 T_STRING);
        case A_REFERENCE:
            return new_key_value(dstrdup("value"), 
                                 make_null_refer(),
                                 T_STRING);
        default:
            UNEXPECTED_VALUE(atom_node->type);
            return NULL;
    }
}

/* Query row value. */
static KeyValue *query_row_value(SelectResult *select_result, ScalarExpNode *scalar_exp, Row *row) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return query_plain_column_value(select_result, scalar_exp->column, row);
        case SCALAR_CALCULATE:
            return query_all_columns_calculate_column_value(select_result, scalar_exp->calculate, row);            
        case SCALAR_VALUE:
            return query_value_item(scalar_exp->value, row);
        case SCALAR_FUNCTION:
            db_log(PANIC, "System logic error at <query_row_value>");
            return NULL;
        default:
            UNEXPECTED_VALUE(scalar_exp->type);
            return NULL;
    }
}

/* Query a Row of Selection,
 * Actually, the Selection is pure-column scalars. */
static Row *query_plain_row_selection(SelectResult *select_result, List *scalar_exp_list, Row *row) {
    if (is_null(row)) 
        return NULL;
    
    Table *table;
    MetaColumn *key_meta_column;
    Row *sub_row;

    table = open_table(row->table_name);
    key_meta_column = get_primary_key_meta_column(table->meta_table);
    sub_row = new_row(copy_value(row->key, key_meta_column->column_type), row->table_name);

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = query_row_value(select_result, scalar_exp, row);
        if (scalar_exp->alias) {
            /* Rename as alias. */
            key_value->key = dstrdup(scalar_exp->alias);
        }
        append_list(sub_row->data, key_value);
    }
    return sub_row;
}

/* Query all columns data. */
static void query_columns_selection(List *scalar_exp_list, SelectResult *select_result) {
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        qfirst(qc) = query_plain_row_selection(select_result, scalar_exp_list, row);
    }
}

/* Check if ScalarExpNode is Function. 
 * If CALCULATE, will check its children. */
static bool is_function_scalar_exp(ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_FUNCTION:
            return true;
        case SCALAR_COLUMN:
            return false;
        case SCALAR_VALUE:
            return false;
        case SCALAR_CALCULATE:
            return is_function_scalar_exp(scalar_exp->calculate->left) 
                || is_function_scalar_exp(scalar_exp->calculate->right);
        default:
            UNEXPECTED_VALUE(scalar_exp->type);
            return false;
    }
}

/* Check if exists function type scalar exp. */
static bool exists_function_scalar_exp(List *scalar_exp_list) {
    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        /* Check self if SCALAR_FUNCTION. */
        ScalarExpNode *scalar_exp = lfirst(lc);
        if (is_function_scalar_exp(scalar_exp))
            return true;
    }
    return false;
}


/* Query selection. */
static void query_with_selection(SelectionNode *selection, SelectResult *select_result) {
    if (selection->all_column)
        return;
    if (exists_function_scalar_exp(selection->scalar_exp_list)) 
        query_fuction_selecton(selection->scalar_exp_list, select_result);
    else 
        query_columns_selection(selection->scalar_exp_list, select_result);
}

/* Get TableExpNode condition. 
 * If exists where clause, return its condition.
 * Else, return NULL.
 * */
static inline ConditionNode *get_table_exp_condition(TableExpNode *table_exp) {
    WhereClauseNode *where_clause = table_exp->where_clause;
    if (where_clause)
        return where_clause->condition;
    else 
        return NULL;
}

static void before_query_condition(SelectParam *selectParam) {
    if (selectParam->onlyAll) {
        db_send("[");
    }
}

static void after_query_condition(SelectParam *selectParam) {
    if (selectParam->onlyAll) {
        db_send("]");
    }
}

/* Query with condition when multiple table. */
static SelectResult *query_multi_table_with_condition(SelectNode *select_node) {
    List *list;
    SelectResult *result;
    ConditionNode *condition;
    SelectParam *selectParam;

    /* If no from clause, return an empty select result. */
    if (is_null(select_node->table_exp->from_clause)) 
        return new_select_result(SELECT_STMT, NULL);

    list = select_node->table_exp->from_clause->from;
    Assert(len_list(list) > 0);
    result = NULL;
    selectParam = optimizeSelect(select_node);
    condition = get_table_exp_condition(select_node->table_exp);

    ListCell *lc;
    foreach (lc, list) {
        TableRefNode *table_ref = lfirst(lc);
        SelectResult *current_result = new_select_result(SELECT_STMT, table_ref->table);

        /* If not define tale alias name, use table name as range variable automatically. */
        current_result->range_variable = table_ref->range_variable 
                                        ? dstrdup(table_ref->range_variable) 
                                        : dstrdup(table_ref->table);
        current_result->derived = result;
        current_result->last_derived = (last_cell(list) == lc);
        
        /* Query with condition to filter satisfied conditions rows. */
        query_with_condition(
            condition, current_result, 
            selectParam->rowHanler, 
            ARG_SELECT_PARAM, selectParam
        );

        result = current_result;
    }

    return result;
}

/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result) {

    /* Check SelectNode valid. */
    check_select_node(select_node);

    /* Query multiple table with conditon and get select result which is after row filtered. */
    SelectResult *select_result = query_multi_table_with_condition(select_node);

    /* Query Selection to define row content. */
    query_with_selection(select_node->selection, select_result);

    /* If select all, return all row data. */
    result->rows = QueueSize(select_result->rows);
    result->data = select_result;
    result->success = true;
    result->message = format("Query %d rows data from table '%s' successfully.", 
                             result->rows, 
                             select_result->table_name);

    /* Make up success result. */
    db_log(SUCCESS, "Query %d rows data from table '%s' successfully.", 
           result->rows, 
           select_result->table_name);
}
