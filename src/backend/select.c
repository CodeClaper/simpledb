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
#include <sys/time.h>
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
#include "row.h"
#include "ltree.h"
#include "pager.h"
#include "table.h"
#include "asserts.h"
#include "session.h"
#include "trans.h"
#include "refer.h"
#include "utils.h"
#include "const.h"
#include "timer.h"
#include "compare.h"
#include "instance.h"
#include "jsonwriter.h"
#include "parall.h"
#include "optimizer.h"
#include "tablecache.h"
#include "strheaptable.h"
#include "heaptable.h"

typedef struct SelectFromInternalChildTaskArgs {
    SelectResult *select_result;
    uint32_t page_num;
    uint32_t keys_num;
    SearchConditionNode *condition;
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
#define VALUE_NAME "value"

static KeyValue *QueryFunctionValue(ScalarExpNode *scalar_exp, SelectResult *select_result);
static KeyValue *QueryRowValueItem(ValueItemNode *value_item, Row *row);
static KeyValue *QueryRowValue(SelectResult *select_result, ScalarExpNode *scalar_exp, Row *row);
static Row *QueryColumnsSelectOneRow(SelectResult *select_result, List *scalar_exp_set, Row *row);
static char *SearchTableViaAlias(SelectResult *select_result, char *range_variable);
static KeyValue *QueryRowColumnValue(SelectResult *select_result, ColumnNode *column, Row *row);
static bool InternalNodeForSearchCondition(SelectResult *select_result, void *min_key, void *max_key, SearchConditionNode *condition_node, MetaTable *meta_table);
static bool LeafNodeForSearchCondition(SelectResult *select_result, List *meta_columns, void *tuple, SearchConditionNode *search_condition);


/* Check if LimitClauseNode is full. 
 * LimitClauseNode full means the poffset is greater or equal the offset.
 * */
inline static bool LimitClauseIsFull(SelectParam *selectParam) {
    return non_null(selectParam->limitClause) && 
        (selectParam->offset >= selectParam->limitClause->offset + selectParam->limitClause->rows);
}


/* Check if include internal comparison predicate for Value type. */
static bool InternalNodeForComparisonPredicateValue(SelectResult *select_result, void *min_key, void *max_key, 
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
static bool InternalNodeForComparisonPredicate(SelectResult *select_result, void *min_key, void *max_key, 
                                               ComparisonNode *comparison, MetaTable *meta_table) {
    ColumnNode *column = comparison->column;

    /* Other table query condition regard as true. */
    if (column->range_variable) {
        char *table_mame = SearchTableViaAlias(select_result, column->range_variable);
        if (is_empty(table_mame)) {
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
            return InternalNodeForComparisonPredicateValue(
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

/* Check if the internal meets predicate. */
static bool InternalNodeForPredicata(SelectResult *select_result, void *min_key, void *max_key, 
                                     PredicateNode *predicate, MetaTable *meta_table) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return InternalNodeForComparisonPredicate(
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

/* Check if the internal node meets the boolean primary. */
static bool InternalNodeForBooleanPrimary(SelectResult *select_result, void *min_key, void *max_key, 
                                          BooleanPrimaryNode *boolean_primary, MetaTable *meta_table) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return InternalNodeForPredicata(select_result, min_key, max_key, boolean_primary->predicate, meta_table);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return InternalNodeForSearchCondition(select_result, min_key, max_key, boolean_primary->search_condition, meta_table);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    } 
}

/* Check if the internal node meets the boolean test. */
static bool InternalNodeForBooleanTest(SelectResult *select_result, void *min_key, void *max_key, 
                                       BooleanTestNode *boolean_test, MetaTable *meta_table) {
    bool bool_primary_value = InternalNodeForBooleanPrimary(select_result, min_key, max_key, boolean_test->boolean_primary, meta_table);
    switch (boolean_test->type) {
        case NONE_TRUE_VALUE: 
            return bool_primary_value;
        case IS_TRUTH_VALUE: 
            return bool_primary_value == boolean_test->truth_value;
        case IS_NOT_TRUTH_VALUE: 
            return bool_primary_value != boolean_test->truth_value;
        default:
            UNEXPECTED_VALUE(boolean_test->type);
            return false;
    }
}

/* Check if the internal node meets the boolean fator. */
static bool InternalNodeForBooleanFactor(SelectResult *select_result, void *min_key, void *max_key, 
                                         BooleanFactorNode *boolean_factor, MetaTable *meta_table) {
    return boolean_factor->is_not
        ? !InternalNodeForBooleanTest(select_result, min_key,  max_key, boolean_factor->boolean_test, meta_table)
        : InternalNodeForBooleanTest(select_result, min_key,  max_key, boolean_factor->boolean_test, meta_table);
}

/* Check if the internal node meets the boolean term. */
static bool InternalNodeForBooleanTerm(SelectResult *select_result, void *min_key, void *max_key, 
                                       BooleanTermNode *boolean_term, MetaTable *meta_table) {
    return boolean_term->and_boolean_term == NULL 
        ? InternalNodeForBooleanFactor(select_result, min_key,  max_key, boolean_term->boolean_factor, meta_table)
        : InternalNodeForBooleanTerm(select_result,  min_key, max_key,  boolean_term->and_boolean_term,  meta_table) && 
            InternalNodeForBooleanFactor(select_result, min_key,  max_key, boolean_term->boolean_factor, meta_table);
}

/* Check if the internal node meet the search condition. */
static bool InternalNodeForSearchCondition(SelectResult *select_result, void *min_key, void *max_key, 
                                           SearchConditionNode *condition_node, MetaTable *meta_table) {
    /* If there is no condition, of course meets, so just return true. */
    if (condition_node == NULL)
        return true;

    return condition_node->or_search_condition == NULL 
        ? InternalNodeForBooleanTerm(select_result,  min_key, max_key,  condition_node->boolean_term,  meta_table)
        : InternalNodeForBooleanTerm(select_result,  min_key, max_key,  condition_node->boolean_term,  meta_table) || 
             InternalNodeForSearchCondition(select_result,  min_key, max_key,  condition_node->or_search_condition,  meta_table);
}

/* Check the row predicate for column. */
static bool ValueForPredicateColumn(SelectResult *select_result, List *meta_columns, void *tuple, void *value, 
                                    ColumnNode *column, CompareType type, MetaColumn *meta_column) {
    char *table_name;
    MetaColumn *target_meta_column;
    
    /* Find table name. Maybe not found when sing-table query, 
     * there is no range_variable. */
    table_name = SearchTableViaAlias(select_result, column->range_variable);

    /* Find meta column. */
    target_meta_column = is_empty(table_name) 
            ? NameFindMetaColumn(meta_columns, column->column_name)
            : TableColumnNameFindMetaColumn(meta_columns, table_name, column->column_name);
    if (target_meta_column == NULL) {
        db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
               column->range_variable, 
               column->column_name);
        return false;
    }
    
    /* Check if the target column equals the meta column. 
     * If yes, it means compare itself, just return true. */
    if (target_meta_column == meta_column)
        return true;
    
    /* Get the target value and eval. */
    void *target_value = get_value_in_tuple(tuple, target_meta_column);
    return eval(type, value, target_value, meta_column->column_type);
}

/* Check the row predicate for value. */
static bool ValueForPredicateValue(SelectResult *select_result, void *value, ValueItemNode *value_item, 
                                      CompareType type, MetaColumn *meta_column) {
    void *target = get_value_from_value_item_node(value_item, meta_column);
    return eval(type, value, target, meta_column->column_type);
}

/* Check if the tuple meets predicate. */
static bool TupleForPredicate(SelectResult *select_result, List *meta_columns, void *tuple, 
                              ColumnNode *column, ComparisonNode *comparison) {
    MetaColumn *meta_column;
    void *value;

    /* Fint the corresponding meta column. */
    if (column->meta_column != NULL)
        meta_column = column->meta_column;
    else {
        if (column->range_variable) {
            char *rtable_name = SearchTableViaAlias(select_result, column->range_variable);
            if (is_empty(rtable_name)) {
                db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
                       column->range_variable, column->column_name);
                return false;
            }
            meta_column = TableColumnNameFindMetaColumn(meta_columns, rtable_name, column->column_name);
        } else 
            meta_column = NameFindMetaColumn(meta_columns, column->column_name);
        column->meta_column = meta_column;
    }

    if (meta_column == NULL) {
        db_log(ERROR, "Not found column '%s'. ", column->column_name);
        return false;
    }
    
    /* Get value in tuple. */
    value = get_value_in_tuple(tuple, meta_column);

    if (column->has_sub_column && column->sub_column) {
        /* Just check, if column has sub column, it must be Reference type. */
        Assert(meta_column->column_type == T_REFERENCE);
        /* Get subrow, and recursion. */
        void *sub_tuple = DefineTuple((Refer *) value);
        Table *sub_table = open_table(meta_column->table_name);
        return TupleForPredicate(
            select_result, 
            sub_table->meta_table->meta_columns,
            sub_tuple, 
            column->sub_column, 
            comparison
        ); 
    } else if (column->has_sub_column && column->scalar_exp_list) {
        db_log(ERROR, "Not support support sub column for pridicate.");
        return false;
    } else {
        ScalarExpNode *comparison_value = comparison->value;
        switch (comparison_value->type) {
            case SCALAR_COLUMN:
                return ValueForPredicateColumn(
                    select_result, meta_columns, tuple, 
                    get_real_value(value, meta_column->column_type), 
                    comparison_value->column, 
                    comparison->type, 
                    meta_column
                );    
            case SCALAR_VALUE: 
                return ValueForPredicateValue(
                    select_result, 
                    get_real_value(value, meta_column->column_type),
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
    /* When column skip test, 
     * it may exists in other tables when multi-table query. */
    return true;
}


/* Check if include leaf node satisfy comparison predicate. */
static bool LeafNodeForComparisonPredicate(SelectResult *select_result, List *meta_columns, 
                                           void *tuple, ComparisonNode *comparison) {
    return TupleForPredicate(select_result, meta_columns, tuple, comparison->column, comparison);
}

/* Check if value in value list. */
static bool ValueInValueList(List *value_list, void *value, MetaColumn *meta_column) {
    ListCell *lc;
    foreach (lc, value_list) {
        void *target = get_value_from_value_item_node(lfirst(lc), meta_column);
        if (equal(value, target, meta_column->column_type))
            return true;
    }
    return false;
}

/* Check if include leaf node satisfy in predicate. */
static bool LeafNodeForInPredicate(SelectResult *select_result, void *tuple, InNode *in_node) {
    Table *table = open_table(select_result->table_name);
    Assert(table != NULL);
    MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, in_node->column->column_name);
    if (meta_column != NULL)
        return ValueInValueList(
            in_node->value_list, 
            get_real_value(get_value_in_tuple(tuple, meta_column), meta_column->column_type), 
            meta_column
        );
    return false;
}

/* Check if value is like string value. */
static bool ValueLikeStringValue(char *value, char *target) {
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
static bool LeafNodeForLikePredicate(SelectResult *select_result, void *tuple, LikeNode *like_node) {
    Table *table = open_table(select_result->table_name);
    MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, like_node->column->column_name);
    if (meta_column != NULL) {
        void *target_value = get_value_from_value_item_node(like_node->value, meta_column);
        void *value = get_value_in_tuple(tuple, meta_column);
        return ValueLikeStringValue(get_real_value(value, meta_column->column_type), target_value);
    }
    return false;
}

/* Check if the leaf node meets predicate. */
static bool LeafNodeForPredicate(SelectResult *select_result, List *meta_columns, 
                                 void *tuple, PredicateNode *predicate) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return LeafNodeForComparisonPredicate(select_result, meta_columns, tuple, predicate->comparison);
        case PRE_IN:
            return LeafNodeForInPredicate(select_result, tuple, predicate->in);
        case PRE_LIKE:
            return LeafNodeForLikePredicate(select_result, tuple, predicate->like);
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }
}

/* If the leaf node meets boolean primary. */
static bool LeafNodeForBooleanPrimary(SelectResult *select_result, List *meta_columns, void *tuple, BooleanPrimaryNode *boolean_primary) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return LeafNodeForPredicate(select_result, meta_columns, tuple, boolean_primary->predicate);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return LeafNodeForSearchCondition(select_result, meta_columns, tuple, boolean_primary->search_condition);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    }
}

/* If the leaf node meets the boolean test. */
static bool LeafNodeForBooleanTest(SelectResult *select_result, List *meta_columns, void *tuple, BooleanTestNode *boolean_test) {
    bool boolean_primary_value = LeafNodeForBooleanPrimary(select_result, meta_columns, tuple, boolean_test->boolean_primary);
    switch (boolean_test->type) {
        case NONE_TRUE_VALUE:
            return boolean_primary_value;
        case IS_TRUTH_VALUE:
            return boolean_primary_value == boolean_test->truth_value;
        case IS_NOT_TRUTH_VALUE:
            return boolean_primary_value != boolean_test->truth_value;
        default:
            UNEXPECTED_VALUE(boolean_test->type);
            return false;
    }
}

/* If the leaf node meets the boolean factor. */
static bool LeafNodeForBooleanFactor(SelectResult *select_result, List *meta_columns, void *tuple, BooleanFactorNode *boolean_factor) {
    return boolean_factor->is_not
        ? !LeafNodeForBooleanTest(select_result, meta_columns,tuple, boolean_factor->boolean_test)
        : LeafNodeForBooleanTest(select_result, meta_columns,tuple, boolean_factor->boolean_test);
}

/* Check if the leaf node meets boolean term. */
static bool LeafNodeForBooleanTerm(SelectResult *select_result, List *meta_columns, void *tuple, BooleanTermNode *boolean_term) {
    return boolean_term->and_boolean_term == NULL
        ? LeafNodeForBooleanFactor(select_result, meta_columns, tuple, boolean_term->boolean_factor)
        : LeafNodeForBooleanTerm(select_result, meta_columns, tuple, boolean_term->and_boolean_term) &&
             LeafNodeForBooleanFactor(select_result, meta_columns, tuple, boolean_term->boolean_factor);
            
}

/* Check if the leaf node meets search condition. */
static bool LeafNodeForSearchCondition(SelectResult *select_result, List *meta_columns, void *tuple, SearchConditionNode *search_condition) {
    /* If there is no condition, of course meets, so just return true. */
    if (search_condition == NULL) 
          return true;
    return search_condition->or_search_condition == NULL 
        ? LeafNodeForBooleanTerm(select_result,  meta_columns,  tuple,  search_condition->boolean_term) 
        : LeafNodeForSearchCondition(select_result,  meta_columns,  tuple,  search_condition->or_search_condition) ||
             LeafNodeForBooleanTerm(select_result,  meta_columns,  tuple,  search_condition->boolean_term);
}

/* Define the tuple by refer. 
 * -------------------------
 * Return the tuple not matter if it is deleted and caller checks if deleted.
 * */
void *DefineTuple(Refer *refer) {
    Assert(refer != NULL);

    /* Check table exists. */
    Table *table = open_table_inner(refer->oid);
    if (table == NULL)
        return NULL;

    /* Check if refer null. */
    if (refer_null(refer))
        return NULL;

    uint32_t key_len, value_len, default_value_len;
    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
                            
    /* Get the leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), refer->page_num);
    LockBuffer(buffer, RW_READERS);
    void *leaf_node = GetBufferPage(buffer);

    void *cell_value = get_leaf_node_cell_value(leaf_node, key_len, value_len, default_value_len, refer->cell_num);
    void *tuple = HeapTableLookupTuple(table, (Refer *) cell_value);
    
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    return tuple;
}

/* Define row by refer. 
 * -------------------
 * Return row not matter if it is deleted, caller check the row if deleted.
 * The row is raw, can`t be freed by caller.
 * */
Row *DefineRow(Refer *refer) {
    Assert(refer != NULL);

    /* Check table exists. */
    Table *table = open_table_inner(refer->oid);
    if (table == NULL)
        return NULL;

    /* Check if refer null. */
    if (refer_null(refer))
        return NULL;

    uint32_t key_len, value_len, default_value_len;
    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
                            
    /* Get the leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), refer->page_num);
    LockBuffer(buffer, RW_READERS);
    void *leaf_node = GetBufferPage(buffer);

    void *destinct = get_leaf_node_cell_value(leaf_node, key_len, value_len, default_value_len, refer->cell_num);
    Row *row = HeapTableLookupRow(table, (Refer *) destinct);
    
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    return row;
}

/* Purge row. 
 * Pruge means to remove the sys-reserved column.
 * */
static void* PurgeRow(Row *row) {
    List *list = row->data;
    /* At least, more 3 sys-reserved column. */
    Assert(list->size > 3);

    /* Delete last 3 sys-reserved items. */
    list_delete_tail(list, 3);

    return row;
}


/* Define row by refer. 
 * Return undelted, filtered row, return NULL if deleted.
 * */
Row *DefineVisibleRow(Refer *refer) {
    Row *row = DefineRow(refer);
    return (RowIsDeleted(row))
        ? NULL
        : PurgeRow(row);
}

/* Merge two tuples. */
static void *MergeTuple(SelectResult *head) {
    if (head->nested == NULL)
        return head->current_tuple;
    else {
        Size offset;
        SelectResult *current;
        void *ntuple;
        
        Assert(head->tuple_size != 0);
        ntuple = dalloc(head->tuple_size);
        offset = 0;
        current = head;

        while (current != NULL) {
            Table *table = open_table(current->table_name);
            memcpy(ntuple + offset, current->current_tuple, table->heap_value_len);
            offset += table->heap_value_len;
            current = current->nested;
        }

        return ntuple;
    }
}

/* Merge meta columns. */
static List *MergeMetaColumns(SelectResult *head) {
    Assert(head != NULL);

    List *meta_columns = create_list(NODE_META_COLUMN);
    SelectResult *current = head;
    Size offset = 0, tuple_size = 0;;

    while (current != NULL) {
        Table *table;
        ListCell *lc;

        table = open_table(current->table_name);
        foreach (lc, table->meta_table->meta_columns) {
            MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
            MetaColumn *duplica = copy_meta_column(meta_column);
            duplica->offset = offset;
            append_list(meta_columns, duplica);
            offset += meta_column->column_length;
            tuple_size += meta_column->column_length;
        }
        current = current->nested;
    }
    head->tuple_size = tuple_size;

    return meta_columns;
}

/* Merge meta columns without system reserved columns. */
static List *MergeMetaColumnsWithoutSys(SelectResult *head) {
    Assert(head != NULL);

    List *meta_columns = create_list(NODE_META_COLUMN);
    SelectResult *current = head;
    Size offset = 0;

    while (current != NULL) {
        Table *table;
        ListCell *lc;

        table = open_table(current->table_name);
        foreach (lc, table->meta_table->meta_columns) {
            MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
            if (!meta_column->sys_reserved) {
                MetaColumn *duplica = copy_meta_column(meta_column);
                duplica->offset = offset;
                append_list(meta_columns, duplica);
            }
            offset += meta_column->column_length;
        }
        current = current->nested;
    }
    return meta_columns;
}

/* Deal with the duplica column name. */
static void DuplicateColumnNameHandler(List *meta_columns) {
    ListCell *lc1, *lc2;
    foreach (lc1, meta_columns) {
        uint32_t times = 0;
        MetaColumn *first = lfirst(lc1);
        foreach (lc2, meta_columns) {
            MetaColumn *second = lfirst(lc2);
            if (lc1 == lc2)
                continue;
            if (streq(second->column_name, first->column_name)) {
                /* Notece: there is still some issue, maybe overflow the MAX_COLUMN_NAME_LEN buffer. */
                if (streq(second->own_table_name, first->own_table_name))
                    memcpy(second->column_name, format("%s(%d)", first->column_name, ++times), MAX_COLUMN_NAME_LEN);
                else {
                    memcpy(first->column_name, format("%s.%s", first->own_table_name, first->column_name), MAX_COLUMN_NAME_LEN);
                    memcpy(second->column_name, format("%s.%s", second->own_table_name, second->column_name), MAX_COLUMN_NAME_LEN);
                }
            }
        } 
    }
}


/* Search table via alias name in SelectResult. 
 * Note: range variable may be table name or table alias name. */
static char *SearchTableViaAlias(SelectResult *select_result, char *range_variable) {
    Assert(select_result != NULL);

    /* Either table name or range variable is equal. */
    if (streq(select_result->table_name, range_variable) || 
            streq(select_result->range_variable, range_variable))
        return select_result->table_name;

    if (select_result->nested)
        return SearchTableViaAlias(select_result->nested, range_variable);

    return NULL;
}

/* Allow to read raw page data. 
 * --------------------------
 * Two condtions:
 * (1) ROW_HANDLER_ARG_TYPE type.
 * (2) only count.
 * */
static bool AllowReaRawPage(ROW_HANDLER_ARG_TYPE type, void *arg) {
    if (type == ARG_SELECT_PARAM) {
        SelectParam *selectParam = (SelectParam *) arg;
        return selectParam->onlyCount;
    }
    return false;
}

/* If allowed scan index. */
static bool AllowScanIndex(ROW_HANDLER_ARG_TYPE type, void *arg) {
    if (type == ARG_SELECT_PARAM) {
        SelectParam *selectParam = (SelectParam *) arg;
        return selectParam->onlyScanIndex;
    }
    return false;
}


/* Scan from leaf node. 
 * -------------------
 * Note that: Scan-index operation only supports for one-table query. */
static void ScanLeafNode(SelectResult *select_result, SearchConditionNode *condition, 
                                uint32_t page_num, Table *table, ROW_HANDLER row_handler, 
                                ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Get cell number, key length and value lenght. */
    uint32_t key_len, value_len, default_value_len, cell_num;
    Buffer buffer;
    void *leaf_node;
    TransEntry *current_trans;

    /* Get leaf node buffer. */
    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);

    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
    cell_num = get_leaf_node_cell_num(leaf_node, default_value_len);
    current_trans = FindTransaction();
    Assert(current_trans != NULL);

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        /* Get leaf node cell value. */
        void *destinct = get_leaf_node_cell_value(leaf_node, key_len, value_len, default_value_len, i);
        Xid created_xid = get_index_created_xid(destinct);
        Xid expired_xid = get_index_expired_xid(destinct);
        if (IsVisibleInner(created_xid, expired_xid, current_trans))
            row_handler(NULL, select_result, type, arg);
    }
    
    /* Release the buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Select through leaf node. */
static void SelectLeafNode(SelectResult *select_result, SearchConditionNode *condition, 
                                  uint32_t page_num, Table *table, ROW_HANDLER row_handler, 
                                  ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Get cell number, key length and value lenght. */
    uint32_t key_len, value_len, default_value_len, cell_num ;
    Buffer buffer;
    void *leaf_node;
    TransEntry *current_trans;
    SelectResult *head, *nested;

    /* If LimitClauseNode full, not continue. */
    if (type == ARG_SELECT_PARAM && LimitClauseIsFull(arg))
        return;

    /* Get leaf node buffer. */
    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = AllowReaRawPage(type, arg) 
            ? GetBufferPage(buffer) 
            : GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);

    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
    cell_num = get_leaf_node_cell_num(leaf_node, default_value_len);
    current_trans = FindTransaction();
    head = select_result->head;
    nested = select_result->nested;

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        /* Get leaf node cell value. */
        void *destinct = get_leaf_node_cell_value(leaf_node, key_len, value_len, default_value_len, i);
        Xid created_xid = get_index_created_xid(destinct);
        Xid expired_xid = get_index_expired_xid(destinct);
        if (!IsVisibleInner(created_xid, expired_xid, current_trans))
            continue;

        /* If satisfied, exeucte row handler function. */
        void *tuple = HeapTableLookupTuple(table, (Refer *) destinct);
        select_result->current_tuple = tuple;

        /* If has nested, deep seek nested. */
        if (nested != NULL) {
            QueryUnderSearchCondition(condition, nested, row_handler, type, arg);
            continue;
        }

        List *columns;
        void *ntuple;

        if (head->columns != NULL) {
            columns = head->columns;
        } else {
            columns = MergeMetaColumns(head);
            head->columns = columns;
        }

        ntuple = MergeTuple(head);
        Assert(columns != NIL);
        Assert(ntuple != NULL);
        
        /* Filt the leaf node. */
        if (LeafNodeForSearchCondition(head, columns, ntuple, condition)) 
            row_handler(ntuple, head, type, arg);
        
        /* When nested not null, means ntuple dalloc new memory. */
        if (head->nested != NULL)
            dfree(ntuple);
    }
    
    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Select through internal node. */
static void SelectInternalNode(SelectResult *select_result, SearchConditionNode *condition, 
                                      uint32_t page_num, Table *table, 
                                      ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* If LimitClauseNode full, not continue. */
    if (type == ARG_SELECT_PARAM && LimitClauseIsFull(arg))
        return;

    /* Get the internal node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    void *internal_node = GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);

    /* Get variables. */
    uint32_t key_len, default_value_len, keys_num;
    key_len = table->key_len;
    default_value_len = table->heap_value_len;
    keys_num = get_internal_node_keys_num(internal_node, default_value_len);

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
            void *max_key = get_real_value(get_internal_node_key(internal_node, i, key_len, default_value_len), primary_key_type); 
            void *min_key = (i == 0) 
                        ? NULL 
                        : get_real_value(get_internal_node_key(internal_node, i - 1, key_len, default_value_len), primary_key_type);
            /* Filter the internal node. */
            if (!InternalNodeForSearchCondition(select_result, min_key, max_key, condition, table->meta_table))
                continue;
        }

        /* Check other non-key column */
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, default_value_len);
        Assert(child_page_num != 0);
        Buffer child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
        void *node = GetBufferPage(child_buffer);

        switch (get_node_type(node)) {
            case LEAF_NODE: {
                if (AllowScanIndex(type, arg))
                    ScanLeafNode(
                        select_result, condition, child_page_num, 
                        table, row_handler, type, arg
                    );
                else
                    SelectLeafNode(
                        select_result, condition, child_page_num, 
                        table, row_handler, type, arg
                    );
                break;
            }
            case INTERNAL_NODE:
                SelectInternalNode(
                    select_result, condition, child_page_num, 
                    table, row_handler, type, arg
                );
                break;
            default:
                db_log(PANIC, "Unknown node type.");
                break;
        }
        
        /* Release the child buffer. */
        ReleaseBuffer(child_buffer);
    }

    /* Don`t forget the right child. */
    /* Fetch right child. */
    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, default_value_len);
    Buffer right_child_buffer = ReadBuffer(GET_TABLE_OID(table), right_child_page_num);
    void *right_child = GetBufferPage(right_child_buffer);
    switch (get_node_type(right_child)) {
        case LEAF_NODE: {
            if (AllowScanIndex(type, arg))
                ScanLeafNode(
                    select_result, 
                    condition, right_child_page_num, 
                    table, row_handler, type, arg
                );
            else
                SelectLeafNode(
                    select_result, 
                    condition, right_child_page_num, 
                    table, row_handler, type, arg
                );
            break;
        }
        case INTERNAL_NODE:
            SelectInternalNode(
                select_result, 
                condition, right_child_page_num, 
                table, row_handler, type, arg
            );
            break;
        default:
            db_log(PANIC, "Unknown node type.");
            break;
    }
 
    free_block(internal_node); 

    /* Release buffers. */
    ReleaseBuffer(right_child_buffer);
    ReleaseBuffer(buffer);
}

/* The task for select internal node in async. */
static void SelectInternalNodeChildTask(void *taskArg) {
    Assert(taskArg != NULL);

    SelectFromInternalChildTaskArgs *args = (SelectFromInternalChildTaskArgs *) taskArg;
    uint32_t child_page_num = args->page_num;
    SelectResult *select_result = args->select_result;
    SearchConditionNode *condition = args->condition;
    Table *table = args->table;
    ROW_HANDLER row_handler = args->row_handler;
    ROW_HANDLER_ARG_TYPE type = args->type;
    void *arg = args->arg;
    Buffer child_buffer;
    void *child_node;

    child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
    child_node = GetBufferPage(child_buffer);

    switch (get_node_type(child_node)) {
        case LEAF_NODE:
            SelectLeafNode(
                select_result, condition, child_page_num, 
                table, row_handler, type, arg
            );
            break;
        case INTERNAL_NODE:
            SelectInternalNode(
                select_result, condition, child_page_num, 
                table, row_handler, type, arg
            );
            break;
        default:
            db_log(PANIC, "Unknown node type.");
            break;
    }

    /* Release the child buffer. */
    ReleaseBuffer(child_buffer);
}

/* Select through internal node. */
static void SelectInternalNodeAsync(SelectResult *select_result, SearchConditionNode *condition, 
                                            uint32_t page_num, Table *table, 
                                            ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {

    /* If LimitClauseNode full, not continue. */
    if (non_null(arg) && LimitClauseIsFull(arg))
        return;

    /* Get the internal node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);

    /* Get variables. */
    uint32_t key_len, value_len, keys_num;
    key_len = table->key_len;
    value_len = table->index_value_len;
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
        if (!InternalNodeForSearchCondition(select_result, min_key, max_key, condition, table->meta_table))
            continue;
        
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
        selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name, true);
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
    selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name, true);
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
    ParallelCompute(4, taskNum, SelectInternalNodeChildTask, (void **)taskArgs);

    /* Summary select result. */
    for (i = 0; i < taskNum; i++) {
        SelectResult *result = selectResults[i];
        printf("%d\t", result->row_size);
        ConcatQueue(select_result->rows, result->rows);
        select_result->row_size += result->row_size;
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
static bool AsyncCondition(SelectResult *select_result) {
    return false;
    /*return select_result->stype == SELECT_STMT && */
    /*            TableNameExistsInCache(select_result->table_name);*/
}

/* Query with condition inner. */
void QueryUnderSearchConditionInner(Oid oid, SearchConditionNode *condition, SelectResult *select_result, 
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
        case LEAF_NODE: {
            if (AllowScanIndex(type, arg))
                ScanLeafNode(
                    select_result, condition, table->root_page_num, 
                    table, row_handler, type, arg
                );
            else
                SelectLeafNode(
                    select_result, condition, table->root_page_num, 
                    table, row_handler, type, arg
                );
            break;
        }
        case INTERNAL_NODE: {
            if (AsyncCondition(select_result)) 
                SelectInternalNodeAsync(
                    select_result, condition, table->root_page_num,
                    table, row_handler, type, arg
                );
            else
                SelectInternalNode(
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
void QueryUnderSearchCondition(SearchConditionNode *condition, SelectResult *select_result, 
                               ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Check if table exists. */
    Table *table = open_table(select_result->table_name);
    if (table == NULL) {
        db_log(ERROR, "Table %s not exist.", select_result->table_name);
        return;
    }
    QueryUnderSearchConditionInner(GET_TABLE_OID(table), condition, select_result, row_handler, type, arg);
}


/* Convert column value to search condition. */
static SearchConditionNode *ColumnValueConvertCondition(MetaColumn *meta_column, void *value) {
    SearchConditionNode *search_condition = instance(SearchConditionNode);
    BooleanTermNode *boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *predicate = instance(PredicateNode);

    /* Assemble the predicate. */
    predicate->type = PRE_COMPARISON;
    predicate->comparison = instance(ComparisonNode);
    predicate->comparison->type = O_EQ;
    predicate->comparison->column = instance(ColumnNode);
    predicate->comparison->column->column_name = dstrdup(meta_column->column_name);
    predicate->comparison->value = instance(ScalarExpNode);
    predicate->comparison->value->type = SCALAR_VALUE;
    predicate->comparison->value->value = instance(ValueItemNode);
    predicate->comparison->value->value->type = V_ATOM;
    predicate->comparison->value->value->value.atom = combine_atom_node(meta_column, value);

    /* Assemble All. */
    boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    boolean_primary->predicate = predicate;
    boolean_test->type = NONE_TRUE_VALUE;
    boolean_test->boolean_primary = boolean_primary;
    boolean_factor->is_not = false;
    boolean_factor->boolean_test = boolean_test;
    boolean_term->boolean_factor = boolean_factor;
    search_condition->boolean_term = boolean_term;

    return search_condition;
}


/* Query with column and value. 
 * ---------------------------
 * This function will query table with column-value condition.
 * And return the SelectResult which freed by caller. 
 * */
SelectResult *SelectWithColumnValue(Oid oid, MetaColumn *meta_column, void *value) {
    /* Check if table exists. */
    Table *table = open_table_inner(oid);
    Assert(table != NULL);
    SearchConditionNode *condtion = ColumnValueConvertCondition(meta_column, value);
    SelectResult *result = new_select_result(SELECT_STMT, GET_TABLE_NAME(table), true);
    QueryUnderSearchConditionInner(oid, condtion, result, SelectRow, ARG_NULL, NULL);
    return result;
}

/* Count number of row, used in the sql function count(1) */
void CountRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {

            acquire_spin_lock(&selectParam->slock);
            /* Double check for concurrency. */
            if (selectParam->offset >= limit_clause->offset && 
                    selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
                select_result->row_size++;
            } 
            release_spin_lock(&selectParam->slock);
        }

        __sync_fetch_and_add(&selectParam->offset, 1);
    } 
    else {
        select_result->row_size++;
    }
}

/* Select tuple data. */
void SelectTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
            acquire_spin_lock(&selectParam->slock);
            /* Double check for concurrency. */
            if (selectParam->offset >= limit_clause->offset && 
                    selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
                AppendQueue(select_result->tuples, tuple);
                select_result->row_size++;
            }
            release_spin_lock(&selectParam->slock);
        }
        __sync_fetch_and_add(&selectParam->offset, 1);
    } else {
        AppendQueue(select_result->tuples, tuple);
        select_result->row_size++;
    }
}

/* Select row data. */
void SelectRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Row *row = GenerateRowInner(tuple, select_result->columns);
    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
            acquire_spin_lock(&selectParam->slock);
            /* Double check for concurrency. */
            if (selectParam->offset >= limit_clause->offset && 
                    selectParam->offset < (limit_clause->offset + limit_clause->rows)) {
                AppendQueue(select_result->rows, row);
                select_result->row_size++;
            }
            release_spin_lock(&selectParam->slock);
        }
        __sync_fetch_and_add(&selectParam->offset, 1);
    } else {
        AppendQueue(select_result->rows, row);
        select_result->row_size++;
    }
}

/* Output tuple data. 
 * --------------
 * This funtion will directly output the tuple data instead of conveting to row data.
 * */
void OutputTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    List *display_columns;

    /* Define the display columns. */
    if (select_result->display_colums != NIL) 
        display_columns = select_result->display_colums;
    else {
        display_columns = MergeMetaColumnsWithoutSys(select_result);
        DuplicateColumnNameHandler(display_columns);
        select_result->display_colums = display_columns;
    }

    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectParam *) arg)->limitClause != NULL) 
    {
        SelectParam *selectParam = (SelectParam *) arg;
        LimitClauseNode *limit_clause = selectParam->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (selectParam->offset >= limit_clause->offset && 
                selectParam->offset < (limit_clause->offset + limit_clause->rows)) 
        {
            if (select_result->first_row_flag)
                select_result->first_row_flag = false;
            else
                db_send(", ");
            json_tuple(display_columns, tuple);
            select_result->row_size++;
        }
        __sync_fetch_and_add(&selectParam->offset, 1);
    }
    else 
    {
        if (select_result->first_row_flag)
            select_result->first_row_flag = false;
        else
            db_send(", ");
        json_tuple(display_columns, tuple);
        select_result->row_size++;
    }
}

/* Calulate column sum value. */
static KeyValue *CalcSumValue(ColumnNode *column, SelectResult *select_result) {
    double sum = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = QueryRowColumnValue(select_result, column, row);
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
    return new_key_value(SUM_NAME, &sum, T_DOUBLE, select_result->table_name);
}


/* Calulate column avg value. */
static KeyValue *CalcAvgValue(ColumnNode *column, SelectResult *select_result) {
    double sum = 0;
    double avg = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = QueryRowColumnValue(select_result, column, row);
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
    return new_key_value(AVG_NAME, &avg, T_DOUBLE, select_result->table_name);
}


/* Calulate column max value.*/
static KeyValue *CalcMaxValue(ColumnNode *column, SelectResult *select_result) {
    void *max_value = NULL;
    DataType data_type = T_UNKNOWN;

    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = QueryRowColumnValue(select_result, column, row);
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

    return new_key_value(MAX_NAME, max_value, data_type, select_result->table_name);
}

/* Calulate column max value.*/
static KeyValue *CalcMinValue(ColumnNode *column, SelectResult *select_result) {
    void *min_value = NULL;
    DataType data_type = T_UNKNOWN;

    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = QueryRowColumnValue(select_result, column, row);
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

    return new_key_value(MIN_NAME, min_value, data_type, select_result->table_name);
}


/* Query count function. */
static KeyValue *QueryCountFunctionValue(FunctionValueNode *value, SelectResult *select_result) {
    uint32_t row_size = select_result->row_size;
    return new_key_value(COUNT_NAME, &row_size, T_INT, select_result->table_name);
}

/* Query sum function. */
static KeyValue *QuerySumFunctionValue(FunctionValueNode *value, SelectResult *select_result) {
    switch (value->value_type) {
        case V_COLUMN: 
            return CalcSumValue(value->column, select_result);
        case V_INT: {
            double sum = value->i_value * (select_result->rows->size);
            return new_key_value(SUM_NAME, &sum, T_DOUBLE, select_result->table_name);
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
KeyValue *QueryAvgFunctionValue(FunctionValueNode *value, SelectResult *select_result) {

    switch (value->value_type) {
        case V_COLUMN:
            return CalcAvgValue(value->column, select_result);
        case V_INT: 
            return new_key_value(AVG_NAME, &value->i_value, T_DOUBLE, select_result->table_name);
        case V_ALL: 
            db_log(ERROR, "Avg function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
}

/* Query max function. */
KeyValue *QueryMaxFunctionValue(FunctionValueNode *value, SelectResult *select_result) {

    switch (value->value_type) {
        case V_COLUMN: 
            return CalcMaxValue(value->column, select_result);
        case V_INT: 
            return new_key_value(MAX_NAME, &value->i_value, T_INT, select_result->table_name);
        case V_ALL: 
            db_log(ERROR, "Max function not support '*'.");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 


/* Query min function. */
KeyValue *QueryMinFunctionValue(FunctionValueNode *value, SelectResult *select_result) {
    switch (value->value_type) {
        case V_COLUMN:
            return CalcMinValue(value->column, select_result);
        case V_INT: 
            return new_key_value(MIN_NAME, &value->i_value, T_INT, select_result->table_name);
        case V_ALL: 
            db_log(PANIC, "Min function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 

/* Query scalar function */
static KeyValue *QueryFunctionColumnValue(FunctionNode *function, SelectResult *select_result) {
    switch (function->type) { 
        case F_COUNT:
            return QueryCountFunctionValue(function->value, select_result);
        case F_SUM:
            return QuerySumFunctionValue(function->value, select_result);
        case F_AVG:
            return QueryAvgFunctionValue(function->value, select_result);
        case F_MAX:
            return QueryMaxFunctionValue(function->value, select_result);
        case F_MIN:
            return QueryMinFunctionValue(function->value, select_result);
        default:
            UNEXPECTED_VALUE("Not implement function yet.");
            return NULL;
    }
}

/* Query column value. */
static KeyValue *QueryRowColumnValue(SelectResult *select_result, ColumnNode *column, Row *row) {
    if (row == NULL) 
        return new_key_value(column->column_name, NULL, T_ROW, select_result->table_name);

    /* Get table name via alias name. */
    char *table_name = SearchTableViaAlias(select_result, column->range_variable);
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
                Row *sub_row = DefineVisibleRow(refer);
                if (column->has_sub_column && column->sub_column) {
                    KeyValue *sub_key_value = QueryRowColumnValue(select_result, column->sub_column, sub_row);
                    return sub_key_value;
                } else if (column->has_sub_column && column->scalar_exp_list) {
                    Row *filtered_subrow = QueryColumnsSelectOneRow(select_result, column->scalar_exp_list, sub_row);
                    return new_key_value(column->column_name, filtered_subrow, T_ROW, table_name);
                } else if (!column->has_sub_column) {
                    return new_key_value(column->column_name, sub_row, T_ROW, table_name); 
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
static KeyValue *CalcAddition(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int32_t sum = *(int32_t *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_INT, NULL);
                }
                case T_LONG: {
                    int64_t sum = *(int32_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float sum = *(int32_t *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sum = *(int32_t *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_DOUBLE, NULL);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sum = *(int64_t *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, NULL);
                }
                case T_LONG: {
                    int64_t sum = *(int64_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float sum = *(int64_t *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sum = *(int64_t *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sum = *(float *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, NULL);
                }
                case T_LONG: {
                    float sum = *(float *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, NULL);
                }
                case T_FLOAT: {
                    float sum = *(float *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sum = *(float *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sum = *(double *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double sum = *(double *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double sum = *(double *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double sum = *(double *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(ADD_NAME, &zero, T_INT, NULL);
        }
    }
}

/* Calulate substraction .*/
static KeyValue *CalcSubstraction(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int32_t sub = *(int32_t *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_INT, NULL);
                }
                case T_LONG: {
                    int64_t sub = *(int32_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float sub = *(int32_t *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sub = *(int32_t *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sub = *(int64_t *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, NULL);
                }
                case T_LONG: {
                    int64_t sub = *(int64_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float sub = *(int64_t *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sub = *(int64_t *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, NULL); 
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sub = *(float *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, NULL);
                }
                case T_LONG: {
                    float sub = *(float *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, NULL);
                }
                case T_FLOAT: {
                    float sub = *(float *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double sub = *(float *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sub = *(double *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double sub = *(double *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double sub = *(double *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double sub = *(double *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, NULL); 
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(SUB_NAME, &zero, T_INT, NULL);
        }
    }
}


/* Calulate multiplication .*/
static KeyValue *CalcMultplication(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t mul = (*(int32_t *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_INT, NULL);
                }
                case T_LONG: {
                    int64_t mul = (*(int32_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float mul = (*(int32_t *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double mul = (*(int32_t *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, NULL);
                }
                case T_LONG: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, NULL);
                }
                case T_FLOAT: {
                    float mul = (*(int64_t *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul,  T_FLOAT, NULL);
                }
                case T_DOUBLE: {
                    double mul = (*(int64_t *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_LONG, NULL);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float mul = (*(float *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, NULL);
                }
                case T_LONG: {
                    float mul = (*(float *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, NULL);
                }
                case T_FLOAT: {
                    float mul = (*(float *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, NULL); 
                }
                case T_DOUBLE: {
                    double mul = (*(float *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double mul = (*(double *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double mul = (*(double *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double mul = (*(double *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double mul = (*(double *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(MUL_NAME, &zero, T_INT, NULL);
        }
    }
}

/* Calulate division .*/
static KeyValue *CalcDivision(KeyValue *left, KeyValue *right) {
    switch (left->data_type) {
        case T_INT: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)(*(int32_t *)left->value) / (*(int32_t *)right->value);
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double div = (double)(*(int64_t *)left->value) / (*(int64_t *)right->value);
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double div = (double)(*(int32_t *)left->value) / (*(float *)right->value);
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double div = (double)(*(int32_t *)left->value) / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(int64_t *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double div = (double)*(int64_t *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double div = (double)*(int64_t *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double div = (double)*(int64_t *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(float *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double div = (double)*(float *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double div = (double)*(float *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double div = (double)*(float *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double div = *(double *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_LONG: {
                    double div = *(double *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                case T_FLOAT: {
                    double div = *(double *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div,  T_DOUBLE, NULL);
                }
                case T_DOUBLE: {
                    double div = *(double *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, NULL);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, NULL);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(DIV_NAME, &zero, T_INT, NULL);
        }
    }
}

/* Query function calculate. */
static KeyValue *QueryFunctionCalcValue(CalculateNode *calculate, SelectResult *select_result) {
    KeyValue *result = NULL;

    KeyValue *left = QueryFunctionValue(calculate->left, select_result);
    KeyValue *right = QueryFunctionValue(calculate->right, select_result);
    
    switch (calculate->type) {
        case CAL_ADD:
            result = CalcAddition(left, right);
            break;
        case CAL_SUB:
            result = CalcSubstraction(left, right);
            break;
        case CAL_MUL:
            result = CalcMultplication(left, right);
            break;
        case CAL_DIV:
            result = CalcDivision(left, right);
            break;
        default:
            UNEXPECTED_VALUE(calculate->type);
            break;
    }
    return result;
}


/* Query column value. */
static KeyValue *QueryFunctionValue(ScalarExpNode *scalar_exp, SelectResult *select_result) {
    Table *table = open_table(select_result->table_name);
    switch (scalar_exp->type) {
        case SCALAR_COLUMN: {
            ColumnNode *column = scalar_exp->column;
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, column->column_name);
            if (QueueIsEmpty(select_result->rows)) {
                return new_key_value(
                    column->column_name, 
                    NULL, 
                    meta_column->column_type,
                    select_result->table_name
                );
            }
            else {
                /* Default, when query function and column data, 
                 * column only return first data. */
                return QueryRowColumnValue(
                    select_result, 
                    column, 
                    qfirst(QueueHead(select_result->rows))
                );
            }
        }
        case SCALAR_FUNCTION:
            return QueryFunctionColumnValue(scalar_exp->function, select_result);
        case SCALAR_CALCULATE:
            return QueryFunctionCalcValue(scalar_exp->calculate, select_result);
        case SCALAR_VALUE: {
            ValueItemNode *value = scalar_exp->value;
            if (QueueIsEmpty(select_result->rows)) 
                return new_key_value(VALUE_NAME, NULL, convert_data_type(value->value.atom->type), NULL);
            else
                return QueryRowValueItem(value, qfirst(QueueHead(select_result->rows)));
        }
        default: {
            UNEXPECTED_VALUE("Unknown scalar type");
            return NULL;
        }
    } 
}

/* Query function data. */
static void QueryFunctionSelection(List *scalar_exp_list, SelectResult *select_result) {
    Row *row = NewRow();

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = QueryFunctionValue(scalar_exp, select_result);        
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
static KeyValue *QueryRowCalcValue(SelectResult *select_result, CalculateNode *calculate, Row *row) {
    KeyValue *result = NULL;

    KeyValue *left = QueryRowValue(select_result, calculate->left, row);
    KeyValue *right = QueryRowValue(select_result, calculate->right, row);

    switch (calculate->type) {
        case CAL_ADD:
            result = CalcAddition(left, right);
            break;
        case CAL_SUB:
            result = CalcSubstraction(left, right);
            break;
        case CAL_MUL:
            result = CalcMultplication(left, right);
            break;
        case CAL_DIV:
            result = CalcDivision(left, right);
            break;
    }
    return result;
}

/* Query value item in scalar_exp. */
static KeyValue *QueryRowValueItem(ValueItemNode *value_item, Row *row) {
    Assert(value_item->type == V_ATOM);
    AtomNode *atom_node = value_item->value.atom;
    switch (atom_node->type) {
        case A_INT:
            return new_key_value(VALUE_NAME, &atom_node->value.intval, T_LONG, NULL);
        case A_BOOL:
            return new_key_value(VALUE_NAME, &atom_node->value.boolval, T_BOOL, NULL);
        case A_FLOAT:
            return new_key_value(VALUE_NAME, &atom_node->value.boolval,  T_DOUBLE, NULL);
        case A_STRING:
            return new_key_value(VALUE_NAME, atom_node->value.strval, T_STRING, NULL);
        case A_REFERENCE:
            return new_key_value(VALUE_NAME, make_null_refer(), T_STRING, NULL);
        default:
            UNEXPECTED_VALUE(atom_node->type);
            return NULL;
    }
}

/* Query row value. */
static KeyValue *QueryRowValue(SelectResult *select_result, ScalarExpNode *scalar_exp, Row *row) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return QueryRowColumnValue(select_result, scalar_exp->column, row);
        case SCALAR_CALCULATE:
            return QueryRowCalcValue(select_result, scalar_exp->calculate, row);            
        case SCALAR_VALUE:
            return QueryRowValueItem(scalar_exp->value, row);
        case SCALAR_FUNCTION:
            db_log(PANIC, "System logic error at <QueryRowValue>");
            return NULL;
        default:
            UNEXPECTED_VALUE(scalar_exp->type);
            return NULL;
    }
}

/* Query a Row of Selection,
 * Actually, the Selection is pure-column scalars. */
static Row *QueryColumnsSelectOneRow(SelectResult *select_result, List *scalar_exp_list, Row *row) {
    if (is_null(row)) 
        return NULL;
    
    Row *sub_row = NewRow();

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = QueryRowValue(select_result, scalar_exp, row);
        if (scalar_exp->alias) {
            /* Rename as alias. */
            key_value->key = dstrdup(scalar_exp->alias);
        }
        append_list(sub_row->data, key_value);
    }
    return sub_row;
}

/* Query all columns data. */
static void QueryColumnsSelection(List *scalar_exp_list, SelectResult *select_result) {
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        qfirst(qc) = QueryColumnsSelectOneRow(select_result, scalar_exp_list, row);
    }
}

/* Check if ScalarExpNode is Function. 
 * If CALCULATE, will check its children. */
static bool IsFunctionScalarExp(ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_FUNCTION:
            return true;
        case SCALAR_COLUMN:
            return false;
        case SCALAR_VALUE:
            return false;
        case SCALAR_CALCULATE:
            return IsFunctionScalarExp(scalar_exp->calculate->left) 
                || IsFunctionScalarExp(scalar_exp->calculate->right);
        default:
            UNEXPECTED_VALUE(scalar_exp->type);
            return false;
    }
}

/* Check if exists function type scalar exp. */
static bool FunctionScalarExpExists(List *scalar_exp_list) {
    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        /* Check self if SCALAR_FUNCTION. */
        ScalarExpNode *scalar_exp = lfirst(lc);
        if (IsFunctionScalarExp(scalar_exp))
            return true;
    }
    return false;
}


/* Query selection. */
static void QueryWithSelection(SelectionNode *selection, SelectResult *select_result) {
    /* For all column, data has stream out by <query_row>*/
    if (selection->all_column)
        return;
    if (FunctionScalarExpExists(selection->scalar_exp_list)) 
        QueryFunctionSelection(selection->scalar_exp_list, select_result);
    else 
        QueryColumnsSelection(selection->scalar_exp_list, select_result);
}

/* Get TableExpNode condition. 
 * If exists where clause, return its condition.
 * Else, return NULL. */
static inline SearchConditionNode *GetTableExpSeachCondition(TableExpNode *table_exp) {
    WhereClauseNode *where_clause = table_exp->where_clause;
    if (where_clause)
        return where_clause->condition;
    else 
        return NULL;
}

/* Do before query condition. */
static void DoBeforeQuerySeachCondition(SelectParam *selectParam) {
    if (selectParam->onlyAll && selectParam->stmt_type == SELECT_STMT) {
        db_send("{ \"success\": true, ");
        db_send("\"data\": [");
    }
}

/* Do after query condition. */
static void DoAfterQuerySearchCondition(SelectParam *selectParam, SelectResult *selectResult, DBResult *dbresult) {
    if (selectParam->onlyAll && selectParam->stmt_type == SELECT_STMT) {
        dbresult->hasOutput = true;
        /* Calulate duration. */
        gettimeofday(&dbresult->end_time, NULL);
        dbresult->duration = time_span(dbresult->end_time, dbresult->start_time);
        db_send("], ");
        db_send("\"rows\": %d,", selectResult->row_size);
        db_send("\"message\": \"Query %d rows data from table '%s' successfully.\", ", selectResult->row_size, selectResult->table_name);
        db_send("\"duration\": %lf }", dbresult->duration);
    }
}

/* Query with condition when multiple table. */
static SelectResult *QueryMultiTableUnderSearchCondition(SelectNode *select_node, DBResult *dbresult) {
    List *table_list;
    SelectResult *head, *pres;
    SearchConditionNode *condition;
    SelectParam *selectParam;

    /* If no from clause, return an empty select result. */
    if (is_null(select_node->table_exp->from_clause)) 
        return new_select_result(SELECT_STMT, NULL, true);

    table_list = select_node->table_exp->from_clause->from;
    Assert(len_list(table_list) > 0);
    head = NULL;
    pres = NULL;
    selectParam = optimizeSelect(select_node, dbresult->stmt_type);
    condition = GetTableExpSeachCondition(select_node->table_exp);

    /* Build the chain of SelectResult. */
    ListCell *lc;
    foreach (lc, table_list) {
        TableRefNode *table_ref = lfirst(lc);
        SelectResult *current_result = new_select_result(SELECT_STMT, table_ref->table, false);

        /* If not define tale alias name, use table name as range variable automatically. */
        current_result->range_variable = table_ref->range_variable 
                                        ? dstrdup(table_ref->range_variable) 
                                        : dstrdup(table_ref->table);
        if (head == NULL)
            head = current_result;

        if (pres != NULL)
            pres->nested = current_result;
    
        current_result->head = head;
        pres = current_result;
    }

    /* Do before query condition. */
    DoBeforeQuerySeachCondition(selectParam);

    /* Do query condition. */
    QueryUnderSearchCondition(
        condition, head, 
        selectParam->rowHanler, 
        ARG_SELECT_PARAM, selectParam
    );

    /* Do after query condition. */
    DoAfterQuerySearchCondition(selectParam, head, dbresult);

    return head;
}

/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result) {
    /* Check SelectNode valid. */
    check_select_node(select_node);

    /* Query multiple table with conditon and get select result which is after row filtered. */
    SelectResult *select_result = QueryMultiTableUnderSearchCondition(select_node, result);

    /* Query Selection to define row content. */
    QueryWithSelection(select_node->selection, select_result);

    /* If select all, return all row data. */
    result->rows = select_result->row_size;
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
