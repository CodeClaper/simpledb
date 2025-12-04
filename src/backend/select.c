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
#include "log.h"
#include "mmgr.h"
#include "bufpool.h"
#include "meta.h"
#include "row.h"
#include "tuple.h"
#include "func.h"
#include "ltbase.h"
#include "ltindex.h"
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
#include "systable.h"
#include "strheaptable.h"
#include "heaptable.h"

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


static KeyValue *QueryFunctionValue(ScalarExpNode *scalar_exp, SelectResult *select_result, SelectPlan *select_plan);
static KeyValue *QueryRowValueItem(ValueItemNode *value_item, Row *row);
static KeyValue *QueryRowValue(SelectPlan *select_plan, ScalarExpNode *scalar_exp, Row *row);
static KeyValue *QueryTupleValue(SelectPlan *select_plan, List *meta_columns, ScalarExpNode *scalar_exp, void *tuple);
static char *SearchTableNameViaAlias(SelectPlan *select_plan, char *alias_name);
static Row *QueryColumnsSelectOneRow(SelectPlan *select_plan, List *scalar_exp_set, Row *row);
static KeyValue *QueryRowColumnValue(SelectPlan *select_plan, ColumnNode *column, Row *row);
static bool InternalNodeForSearchCondition(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, SearchConditionNode *search_condition);
static bool LeafNodeForSearchCondition(SelectPlan *select_plan, List *meta_columns, void *tuple, SearchConditionNode *search_condition);
static KeyValue *CalcAddition(KeyValue *left, KeyValue *right);
static KeyValue *CalcSubstraction(KeyValue *left, KeyValue *right);
static KeyValue *CalcMultplication(KeyValue *left, KeyValue *right);
static KeyValue *CalcDivision(KeyValue *left, KeyValue *right);
static KeyValue *QueryTupleColumnValue(SelectPlan *select_plan, List *meta_columns, ColumnNode *column, void *tuple);
static void SelectUnderCondition(Oid oid, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan);


/* Check if LimitClauseNode is full. 
 * LimitClauseNode full means the poffset >= the offset.
 * */
inline static bool LimitClauseIsFull(SelectPlan *select_plan) {
    return NonNull(select_plan->limitClause) && 
        (select_plan->offset >= select_plan->limitClause->offset + select_plan->limitClause->rows);
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
        return Contains(value, str_dup);
    }
    else if (target[0] == '%')
        return EndWith(value, target + 1);
    else if (target[target_len - 1] == '%') {
        char str_dup[target_len];
        memset(str_dup, 0, target_len);
        memcpy(str_dup, target, target_len - 1);
        return StartWith(value, str_dup);
    } 
    else 
        return StrEq(value, target);
}

static MetaColumn *ColumnNodeFindMetaColumn(SelectPlan *select_plan, List *meta_columns, ColumnNode *column) {
    Table *table;
    char *table_name;
    MetaColumn *target_meta_column;

    /* Find table name. Maybe not found when sing-table query, 
     * there is no range_variable. */
    table_name = SearchTableNameViaAlias(select_plan, column->range_variable);
    if (!StrIsEmpty(table_name)) table = open_table(table_name);

    /* Find meta column. */
    target_meta_column = StrIsEmpty(table_name) 
            ? NameFindMetaColumnInner(meta_columns, column->column_name)
            : TableColumnNameFindMetaColumn(meta_columns, GET_TABLE_OID(table), column->column_name);
    if (target_meta_column == NULL) {
        db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
               column->range_variable, 
               column->column_name);
        return NULL;
    }
    
    return target_meta_column;
}

static Row *QuerySubRowFromSubTuple(SelectPlan *select_plan, List *meta_columns, List *scalar_exp_list, void *sub_tuple) {
    Row *sub_row = NewRow();
    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = QueryTupleValue(select_plan, meta_columns, scalar_exp, sub_tuple);
        if (scalar_exp->alias) {
            /* Rename as alias. */
            key_value->key = dstrdup(scalar_exp->alias);
        }
        append_list(sub_row->data, key_value);
    }
    return sub_row;
}

/* Query tuple for sub column value. */
static KeyValue *QueryTupleColumnValueForSubColumn(SelectPlan *select_plan, MetaColumn *target_meta_column, ColumnNode *column, void *value) {
    Assert(target_meta_column->column_type == T_RID && column->has_sub_column);
    Rid ref_id = *(Rid *) value;
    void *sub_tuple = FetchTupleViaRid(target_meta_column->type_oid, ref_id);
    Table *sub_table = open_table_inner(target_meta_column->type_oid);
    if (column->sub_column != NULL) {
        return QueryTupleColumnValue(select_plan, sub_table->meta_table->meta_columns, column->sub_column, sub_tuple);
    } else if (!list_empty(column->scalar_exp_list)) {
        Row *sub_row = QuerySubRowFromSubTuple(select_plan, sub_table->meta_table->meta_columns, column->scalar_exp_list, sub_tuple);
        return new_key_value(target_meta_column->column_name, sub_row, T_OBJECT, target_meta_column->tid, target_meta_column->type_oid);
    }
    return NULL;
}

/* Query tuple column value. */
static KeyValue *QueryTupleColumnValue(SelectPlan *select_plan, List *meta_columns, ColumnNode *column, void *tuple) {
    MetaColumn *target_meta_column = ColumnNodeFindMetaColumn(select_plan, meta_columns, column);
    void *value = GetComparableValue(TupleFindValue(tuple, target_meta_column), target_meta_column->column_type);
    return column->has_sub_column 
        ? QueryTupleColumnValueForSubColumn(select_plan, target_meta_column, column, value)
        : new_key_value(target_meta_column->column_name, value, target_meta_column->column_type, target_meta_column->tid, target_meta_column->type_oid);
}

/* Query tuple calulate value. */
static KeyValue *QueryTupleCalulateValue(SelectPlan *select_plan, List *meta_columns, CalculateNode *calculate, void *tuple) {
    KeyValue *result, *left, *right;

    result = NULL;
    left = QueryTupleValue(select_plan, meta_columns, calculate->left, tuple);
    right = QueryTupleValue(select_plan, meta_columns, calculate->right, tuple);

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

/* Query tuple value item. */
static KeyValue *QueryTupleValueItem(ValueItemNode *value_item) {
    void *value = ValueItemNodeFindValue(value_item);
    return value == NULL 
        ? new_key_value(NULL, value, T_UNKNOWN, OID_ZERO, OID_ZERO)
        : new_key_value(NULL, value, AtomTypeConvertDataType(value_item->value.atom->type), OID_ZERO, OID_ZERO);
}

/* Query tuple function value. */
static KeyValue *QueryTupleFuncitonValue(SelectPlan *select_plan, List *meta_columns, FunctionNode *function, void *tuple) {
    if (IsAggFuncion(function->type))
        db_log(ERROR, "Aggregate function not allowd in where.");
    return NULL;
}

/* Query tuple value. */
static KeyValue *QueryTupleValue(SelectPlan *select_plan, List *meta_columns, ScalarExpNode *scalar_exp, void *tuple) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return QueryTupleColumnValue(select_plan, meta_columns, scalar_exp->column, tuple);
        case SCALAR_CALCULATE:
            return QueryTupleCalulateValue(select_plan, meta_columns, scalar_exp->calculate, tuple);
        case SCALAR_VALUE:
            return QueryTupleValueItem(scalar_exp->value);
        case SCALAR_FUNCTION:
            return QueryTupleFuncitonValue(select_plan, meta_columns, scalar_exp->function, tuple);
        default:
            UNEXPECTED_VALUE(scalar_exp->type);
            return NULL;
    }
}

/* Find rid by refer value. */
static Rid *ReferValueFindRid(ReferValue *refer_value, MetaColumn *meta_column) {
    switch (refer_value->type) {
        case DIRECTLY:
            panic("Logic error");
            break;
        case INDIRECTLY: {
            Rid rid = FetchRefIdUnderCondition(meta_column->type_oid, refer_value->condition); 
            return copy_value(&rid, T_RID);
        }
        default:
            UNEXPECTED_VALUE(refer_value->type);
    }
    return NULL;
}

static inline Rid *ScalarExpFindRid(ScalarExpNode *scalar_exp, MetaColumn *meta_column) {
    ReferValue *refer_value = scalar_exp->value->value.atom->value.referval;
    return ReferValueFindRid(refer_value, meta_column);
}

static bool ValueItemIsRidValue(ValueItemNode *value_item) {
    switch (value_item->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item->value.atom;
            return atom_node->type == A_REFERENCE;
        }
        case V_ARRAY: {
            ListCell *lc;
            foreach(lc, value_item->value.value_list) {
                ValueItemNode *value_item = (ValueItemNode *)lfirst(lc);
                /* Any of it is ReferValue is true. */
                if (ValueItemIsRidValue(value_item))
                    return true;
            }
        }
        case V_NULL:
            return false;
    }
    return false;
} 

static bool ScalarExpIsRidValue(ScalarExpNode *scalar_exp) {
    if (scalar_exp->type == SCALAR_VALUE) {
        ValueItemNode *value_item = scalar_exp->value;
        return ValueItemIsRidValue(value_item);
    }
    return false;
}

/* If satisfy columnn and refer value in comparison. */
static bool SatisfyColumnAndReferValueCompparison(ScalarExpNode *left, ScalarExpNode *right) {
    if (ScalarExpIsRidValue(left)) {
        if (right->type == SCALAR_COLUMN)
            return true;
        else
            db_log(ERROR, "Refer value must compare with column.");
    } else if (ScalarExpIsRidValue(right)) {
        if (left->type == SCALAR_COLUMN)
            return true;
        else
            db_log(ERROR, "Refer value must compare with column.");
    }
    return false;
}

static bool ColumnAndReferValueCompparison(SelectPlan *select_plan, List *meta_columns, 
                                           CompareType compare_type, ScalarExpNode *left, 
                                           ScalarExpNode *right, void *tuple) {
    MetaColumn *target_meta_column;
    void *source, *target;

    if (ScalarExpIsRidValue(right)) {
        target_meta_column = ColumnNodeFindMetaColumn(select_plan, meta_columns, left->column);
        target = ScalarExpFindRid(right, target_meta_column);
        source = TupleFindValue(tuple, target_meta_column);
    } else {
        target_meta_column = ColumnNodeFindMetaColumn(select_plan, meta_columns, right->column);
        source = ScalarExpFindRid(left, target_meta_column);
        target = TupleFindValue(tuple, target_meta_column);
    }

    return eval(compare_type, &source, &target, T_RID);
}

/* Check if include leaf node satisfy comparison predicate. */
static bool LeafNodeForComparisonPredicate(SelectPlan *select_plan, List *meta_columns, void *tuple, ComparisonNode *comparison) {
    /* Do specially for column-refervalue comparison. */
    if (SatisfyColumnAndReferValueCompparison(comparison->left, comparison->right))
        return ColumnAndReferValueCompparison(select_plan, 
                                              meta_columns, 
                                              comparison->type, 
                                              comparison->left, 
                                              comparison->right, 
                                              tuple);
    /* Do others normally. */
    KeyValue *leftVal = QueryTupleValue(select_plan, meta_columns, comparison->left, tuple);
    KeyValue *rightVal = QueryTupleValue(select_plan, meta_columns, comparison->right, tuple);
    return KeyValueEval(comparison->type, leftVal, rightVal);
}

/* Check if include leaf node satisfy in predicate. */
static bool LeafNodeForInPredicate(SelectPlan *select_plan, List *meta_columns, void *tuple, InNode *in_node) {
    MetaColumn *meta_column;
    KeyValue *value, *target;

    meta_column = ColumnNodeFindMetaColumn(select_plan, meta_columns, in_node->column);
    value = QueryTupleColumnValue(select_plan, meta_columns, in_node->column, tuple);

    ListCell *lc;
    foreach (lc, in_node->value_list) {
        target = QueryTupleValueItem((ValueItemNode *) lfirst(lc));
        /* For referenct typew convert ReferValue to rid value.  */
        if (meta_column->column_type == T_RID && target->data_type == T_REFER) {
            target->value = ReferValueFindRid(target->value, meta_column);
            target->data_type = T_RID;
        }
        if (KeyValueEval(O_EQ, value, target))
            return true;
    }
    return false;
}

/* Check if include leaf node satisfy like predicate. */
static bool LeafNodeForLikePredicate(SelectPlan *select_plan, List *meta_columns, void *tuple, LikeNode *like_node) {
    MetaColumn *meta_column = ColumnNodeFindMetaColumn(select_plan, meta_columns, like_node->column);
    void *target_value = ValueItemNodeFindValue(like_node->value);
    void *value = TupleFindValue(tuple, meta_column);
    return ValueLikeStringValue(GetComparableValue(value, meta_column->column_type), target_value);
}

/* Check if the leaf node meets predicate. */
static bool LeafNodeForPredicate(SelectPlan *select_plan, List *meta_columns, void *tuple, PredicateNode *predicate) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return LeafNodeForComparisonPredicate(select_plan, meta_columns, tuple, predicate->comparison);
        case PRE_IN:
            return LeafNodeForInPredicate(select_plan, meta_columns, tuple, predicate->in);
        case PRE_LIKE:
            return LeafNodeForLikePredicate(select_plan, meta_columns, tuple, predicate->like);
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }
}

/* If the leaf node meets boolean primary. */
static bool LeafNodeForBooleanPrimary(SelectPlan *select_plan, List *meta_columns, void *tuple, BooleanPrimaryNode *boolean_primary) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return LeafNodeForPredicate(select_plan, meta_columns, tuple, boolean_primary->predicate);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return LeafNodeForSearchCondition(select_plan, meta_columns, tuple, boolean_primary->search_condition);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    }
}

/* If the leaf node meets the boolean test. */
static bool LeafNodeForBooleanTest(SelectPlan *select_plan, List *meta_columns, void *tuple, BooleanTestNode *boolean_test) {
    bool boolean_primary_value = LeafNodeForBooleanPrimary(select_plan, meta_columns, tuple, boolean_test->boolean_primary);
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
static bool LeafNodeForBooleanFactor(SelectPlan *select_plan, List *meta_columns, void *tuple, BooleanFactorNode *boolean_factor) {
    return boolean_factor->is_not
        ? !LeafNodeForBooleanTest(select_plan, meta_columns,tuple, boolean_factor->boolean_test)
        : LeafNodeForBooleanTest(select_plan, meta_columns,tuple, boolean_factor->boolean_test);
}

/* Check if the leaf node meets boolean term. */
static bool LeafNodeForBooleanTerm(SelectPlan *select_plan, List *meta_columns, void *tuple, BooleanTermNode *boolean_term) {
    return boolean_term->and_boolean_term == NULL
        ? LeafNodeForBooleanFactor(select_plan, meta_columns, tuple, boolean_term->boolean_factor)
        : LeafNodeForBooleanFactor(select_plan, meta_columns, tuple, boolean_term->boolean_factor) &&
            LeafNodeForBooleanTerm(select_plan, meta_columns, tuple, boolean_term->and_boolean_term);
}

/* Check if the leaf node meets search condition. */
static bool LeafNodeForSearchCondition(SelectPlan *select_plan, List *meta_columns, void *tuple, SearchConditionNode *search_condition) {
    /* If there is no condition, of course meets, so just return true. */
    if (search_condition == NULL) 
          return true;
    return search_condition->or_search_condition == NULL 
        ? LeafNodeForBooleanTerm(select_plan, meta_columns, tuple, search_condition->boolean_term) 
        : LeafNodeForSearchCondition(select_plan, meta_columns, tuple, search_condition->or_search_condition) ||
             LeafNodeForBooleanTerm(select_plan, meta_columns, tuple, search_condition->boolean_term);
}

/* Check if the internal comparison meets comparison. */
static bool InternalNodeForComparisonPredicate(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, ComparisonNode *comparison, bool negation) {
    /* Refer value will cause index invalid */
    if (SatisfyColumnAndReferValueCompparison(comparison->left, comparison->right))
        return true;
    
    ScalarExpNode *left = comparison->left;
    ScalarExpNode *right = comparison->right;
    switch (comparison->type) {
        case O_EQ: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                            : true; 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                            : true;
            }
            break;
        }
        case O_NE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? true
                            : KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value);
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? true
                            : KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value); 
            }
            break;
        }
        case O_GT: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_GT, max_key_value, value) 
                            : KeyValueEval(O_LT, min_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LT, min_key_value, value) 
                            : KeyValueEval(O_GT, max_key_value, value);
            }
            break;
        }
        case O_GE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_GE, max_key_value, value) 
                            : KeyValueEval(O_LE, min_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LE, min_key_value, value) 
                            : KeyValueEval(O_GE, max_key_value, value);
            }
            break;
        }
        case O_LT: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LT, min_key_value, value) 
                            : KeyValueEval(O_GT, max_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_GT, max_key_value, value) 
                            : KeyValueEval(O_LT, min_key_value, value); 
            }
            break;
        }
        case O_LE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                KeyValue *value = QueryTupleValueItem(right->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_LE, min_key_value, value) 
                            : KeyValueEval(O_GE, max_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                KeyValue *value = QueryTupleValueItem(left->value);
                if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
                    return !negation 
                            ? KeyValueEval(O_GE, max_key_value, value) 
                            : KeyValueEval(O_LE, min_key_value, value); 
            }
            break;
        }
        default:
            UNEXPECTED_VALUE(comparison->type);
    }

    return true;
}

/* Check if the internal node meets like predicate. */
static bool InternalNodeForLikePredicate(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, LikeNode *like, bool negation) { 
    /* For not like operation, it`s realy hard not to fall into the scope of internal node, 
     * which means everyone in the scope of internal node is like to the target value. 
     * Absolutely, it`s not possible so just return true. */
    if (negation)
        return true;

    ColumnNode *column;
    KeyValue *value;
    char *strVal, *newStrVal;
    Size len;

    column = like->column;
    value = QueryTupleValueItem(like->value);
    strVal = value->value;
    len = strlen(strVal);
    AssertFalse(strVal[0] == '%');

    /* Trim right '%s' character if necessary.  */
    if (strVal[len - 1] == '%') {
        newStrVal = dstrdup(strVal);
        newStrVal[len - 1] = '\0';
        value->value = newStrVal;
    }

    if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
        return !negation 
                ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                : KeyValueEval(O_GE, min_key_value, value) || KeyValueEval(O_LT, max_key_value, value); 
    return true;
}

/* Check if the internal node meets in predicate. */
static bool InternalNodeForInPredicate(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, InNode *in, bool negation) {
    /* For not in operation, it`s realy hard not to fall into the scope of internal node. 
     * Because it`s the whole world escape the in operation target values, so just return true. */
    if (negation)
        return true;

    ColumnNode *column = in->column;
    if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key)) {
        ListCell *lc;
        foreach (lc, in->value_list) {
            KeyValue *value = QueryTupleValueItem((ValueItemNode *) lfirst(lc));
            if (KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value))
                return true;
        }
        return false;
    }
    return true;
}

/* Check if the internal node meets predicate. */
static bool InternalNodeForPredicate(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, PredicateNode *predicate, bool negation) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return InternalNodeForComparisonPredicate(
                select_plan, min_key_value, max_key_value, 
                predicate->comparison, negation
            );
        case PRE_LIKE:
            return InternalNodeForLikePredicate(
                select_plan, min_key_value, max_key_value, 
                predicate->like, negation
            );
        case PRE_IN:
            return InternalNodeForInPredicate(
                select_plan, min_key_value, max_key_value, 
                predicate->in, negation
            );
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }
}

/* Check if the internal node meets the boolean primary. */
static bool InternalNodeForBooleanPrimary(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, BooleanPrimaryNode *boolean_primary, bool negation) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return InternalNodeForPredicate(select_plan, min_key_value, max_key_value, boolean_primary->predicate, negation);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return !negation 
                    ? InternalNodeForSearchCondition(select_plan, min_key_value, max_key_value, boolean_primary->search_condition) 
                    : true;
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    } 
}

/* Check if the internal node meets the boolean test. */
static bool InternalNodeForBooleanTest(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, BooleanTestNode *boolean_test, bool negation) {
    switch (boolean_test->type) {
        case NONE_TRUE_VALUE: 
            return InternalNodeForBooleanPrimary(select_plan, min_key_value, max_key_value, boolean_test->boolean_primary, negation);
        case IS_TRUTH_VALUE: 
            return InternalNodeForBooleanPrimary(
                select_plan, min_key_value, max_key_value, boolean_test->boolean_primary, 
                /* The following is XOR. */
                negation == boolean_test->truth_value
            );
        case IS_NOT_TRUTH_VALUE: 
            return InternalNodeForBooleanPrimary(
                select_plan, min_key_value, max_key_value, boolean_test->boolean_primary, 
                /* The following is XOR. */
                negation != boolean_test->truth_value
            );
        default:
            UNEXPECTED_VALUE(boolean_test->type);
            return true;
    }
}

/* Check if the internal node meets the boolean fator. */
static bool InternalNodeForBooleanFactor(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, BooleanFactorNode *boolean_factor) {
    return InternalNodeForBooleanTest(select_plan, min_key_value, max_key_value, boolean_factor->boolean_test, boolean_factor->is_not);
}

/* Check if the internal node meets the boolean term. */
static bool InternalNodeForBooleanTerm(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, BooleanTermNode *boolean_term) {
    return boolean_term->and_boolean_term == NULL 
        ? InternalNodeForBooleanFactor(select_plan, min_key_value,  max_key_value, boolean_term->boolean_factor)
        : InternalNodeForBooleanFactor(select_plan, min_key_value,  max_key_value, boolean_term->boolean_factor) && 
            InternalNodeForBooleanTerm(select_plan,  min_key_value, max_key_value, boolean_term->and_boolean_term);
}

/* Check if the internal node meet the search condition. */
static bool InternalNodeForSearchCondition(SelectPlan *select_plan, KeyValue *min_key_value, KeyValue *max_key_value, SearchConditionNode *condition) {
    /* If index is invalid, just return true. */
    if (!select_plan->indexValid)
        return true;

    return condition->or_search_condition == NULL 
        ? InternalNodeForBooleanTerm(select_plan, min_key_value, max_key_value, condition->boolean_term)
        : InternalNodeForBooleanTerm(select_plan, min_key_value, max_key_value, condition->boolean_term) || 
            InternalNodeForSearchCondition(select_plan, min_key_value, max_key_value, condition->or_search_condition);
}


/* Define the tuple by refer. 
 * -------------------------
 * Return the tuple not matter if it is deleted and caller checks if deleted.
 * */
void *DefineTuple(Refer *refer) {
    Assert(refer != NULL);

    /* Check table exists. */
    Table *table = open_table_inner(refer->oid);
    if (table == NULL) return NULL;

    /* Check if refer null. */
    if (ReferIsEmpty(refer)) return NULL;

    uint32_t key_len, value_len, default_value_len;
    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
                            
    /* Get the leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), refer->page_num);
    LockBuffer(buffer, RW_READERS);
    void *leaf_node = GetBufferPage(buffer);

    void *cell_value = LeafNodeGetCellValue(leaf_node, key_len, value_len, default_value_len, refer->cell_num);
    void *tuple = HeapTableLookupTuple(refer->oid, (Refer *) cell_value);
    
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    return tuple;
}

/* Purge row. 
 * Pruge means to remove the sys-reserved column.
 * */
static void* PurgeRow(Row *row) {
    List *list = row->data;
    /* At least, more than sys-reserved column. */
    Assert(list->size > SYS_RESERVED_COLUMNS_LENGTH );
    /* Delete last all sys-reserved items. */
    list_delete_tail(list, SYS_RESERVED_COLUMNS_LENGTH);
    return row;
}


/* Define row by refer. 
 * Return undelted, filtered row, return NULL if deleted.
 * */
Row *DefineVisibleRow(Oid toid, Rid ref_id) {
    AssertFalse(ZERO_OID(toid));
    AssertFalse(ZERO_RID(ref_id));
    Row *row = FetchSubRow(toid, ref_id);
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
            if (StrEq(second->column_name, first->column_name)) {
                /* Notece: there is still some issue, maybe overflow the MAX_COLUMN_NAME_LEN buffer. */
                if (second->tid == first->tid)
                    memcpy(second->column_name, FormatStr("%s(%d)", first->column_name, ++times), MAX_COLUMN_NAME_LEN);
                else {
                    Table *first_table = open_table_inner(first->tid);
                    Table *second_table = open_table_inner(second->tid);
                    memcpy(first->column_name, FormatStr("%s.%s", GET_TABLE_NAME(first_table), first->column_name), MAX_COLUMN_NAME_LEN);
                    memcpy(second->column_name, FormatStr("%s.%s", GET_TABLE_NAME(second_table), second->column_name), MAX_COLUMN_NAME_LEN);
                }
            }
        } 
    }
}

/* Search table via alias name in SelectResult. 
 * Note: range variable may be table name or table alias name. */
static char *SearchTableNameViaAlias(SelectPlan *select_plan, char *alias_name) {
    if (select_plan->selectTableList != NIL) {
        ListCell *lc;
        foreach (lc, select_plan->selectTableList) {
            SelectTable *select_table = (SelectTable *)lfirst(lc);
            if (StrEq(select_table->alias_name, alias_name) || StrEq(GET_TABLE_NAME(select_table->table), alias_name))
                return GET_TABLE_NAME(select_table->table);
        }
    }
    return NULL;
}

/* Scan from leaf node. 
 * -------------------
 * Note that: Scan-index operation only supports for one-table query. */
static void ScanForLeafNode(Oid oid, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Table *table;
    Buffer buffer;
    DataType ptype;
    void *leaf_node, *high_key;
    TransEntry *current_trans;
    uint32_t key_len, value_len, default_value_len, cell_num;

    /* Get leaf node buffer. */
    table = open_table_inner(oid);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);

    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
    high_key = NodeGetHighKey(table, leaf_node);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    cell_num = LeafNodeGetCellNum(leaf_node, default_value_len);
    current_trans = FindTransaction();
    Assert(current_trans != NULL);

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        /* Get leaf node cell value. */
        Xid created_xid = LeafNodeGetCellCreatedXid(leaf_node, key_len, value_len, default_value_len, i);
        Xid expired_xid = LeafNodeGetCellExpiredXid(leaf_node, key_len, value_len, default_value_len, i);
        if (IsVisibleInner(created_xid, expired_xid, current_trans))
            select_plan->rowHanler(NULL, select_result, select_plan->type, select_plan->arg);
    }

    /* The only condition to move to sibling:
     * The target node has spliten. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        uint32_t next_sibling = NodeGetNextSibling(table, leaf_node);
        Assert(next_sibling != 0);
        ScanForLeafNode(oid, next_sibling, boundary_key, select_result, select_plan);
    }
    
    /* Release the buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Select through leaf node. */
static void SelectForLeafNode(Oid oid, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Table *table;
    Buffer buffer;
    DataType ptype;
    void *leaf_node, *high_key;
    TransEntry *current_trans;
    SelectResult *head, *nested;
    uint32_t key_len, value_len, default_value_len, cell_num ;

    /* If LimitClauseNode full, not continue. */
    if (LimitClauseIsFull(select_plan)) return;

    table = open_table_inner(oid);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = select_plan->onlyCount 
            ? GetBufferPage(buffer) 
            : GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);

    key_len = table->key_len;
    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;

    cell_num = LeafNodeGetCellNum(leaf_node, default_value_len);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    high_key = NodeGetHighKey(table, leaf_node);
    current_trans = FindTransaction();
    head = select_result->head;
    nested = select_result->nested;

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        /* Get leaf node cell value. */
        void *cell_value = LeafNodeGetCellValue(leaf_node, key_len, value_len, default_value_len, i);
        Xid created_xid = LeafNodeGetCellCreatedXid(leaf_node, key_len, value_len, default_value_len, i);
        Xid expired_xid = LeafNodeGetCellExpiredXid(leaf_node, key_len, value_len, default_value_len, i);

        /* If not visible, skip it. */
        if (!IsVisibleInner(created_xid, expired_xid, current_trans))
            continue;

        /* If satisfied, exeucte row handler function. */
        void *tuple = HeapTableLookupTuple(oid, (Refer *) cell_value);
        select_result->current_tuple = tuple;

        /* If has nested, deep seek nested. */
        if (nested != NULL) {
            QueryUnderSearchCondition(nested, select_plan);
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
        if (LeafNodeForSearchCondition(select_plan, columns, ntuple, select_plan->condition)) 
            select_plan->rowHanler(ntuple, head, select_plan->type, select_plan->arg);
        
        /* When nested not null, means ntuple dalloc new memory. */
        if (head->nested != NULL)
            dfree(ntuple);
    }

    /* The only condition to move to sibling:
     * The target node has spliten. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        uint32_t next_sibling = NodeGetNextSibling(table, leaf_node);
        Assert(next_sibling != 0);
        SelectForLeafNode(oid, next_sibling, boundary_key, select_result, select_plan);
    }
    
    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Select through internal node. */
static void SelectForInternalNode(Oid oid, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Table *table;
    Buffer buffer;
    DataType ptype;
    MetaColumn *primary_meta_column;
    void *internal_node, *high_key;
    uint32_t key_len, default_value_len, keys_num;

    /* If LimitClauseNode full, not continue. */
    if (LimitClauseIsFull(select_plan)) return;

    table = open_table_inner(oid);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    key_len = table->key_len;
    default_value_len = table->heap_value_len;
    keys_num = InternalNodeGetKeysNum(internal_node, default_value_len);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);
    high_key = NodeGetHighKey(table, internal_node);

    /* Loop each interanl node cell to check if satisfy condition. 
     * Note that: get the internal node keys number in each loop.
     * It`s important for reading when inserting in the concurrency scenario.
     * */
    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        /* Check if index column, use index to avoid full text scanning. */
        /* Current internal node cell key as max key, previous cell key as min key, so the the range of values is (min_key, max_key]. */

        void *max_key, *min_key;
        KeyValue *max_key_value, *min_key_value, *child_boundary_key;
        uint32_t child_page_num;

        child_boundary_key = InternalNodeGetCellKey(internal_node, key_len, default_value_len, i);
        max_key = GetComparableValue(child_boundary_key, ptype); 
        min_key = (i == 0) ? NULL : GetComparableValue(InternalNodeGetCellKey(internal_node, key_len, default_value_len, i - 1), ptype);
        max_key_value = new_key_value(primary_meta_column->column_name, max_key, ptype, oid, primary_meta_column->type_oid);
        min_key_value = new_key_value(primary_meta_column->column_name, min_key, ptype, oid, primary_meta_column->type_oid);
        /* Filter the internal node. */
        if (!InternalNodeForSearchCondition(select_plan, min_key_value, max_key_value, select_plan->condition))
            continue;

        child_page_num = InternalNodeGetCellValue(internal_node, key_len, default_value_len, i);
        Assert(child_page_num != 0);
        SelectUnderCondition(oid, child_page_num, child_boundary_key, select_result, select_plan);
    }

    /* Don`t forget the right child. */
    /* Fetch right child. */
    uint32_t right_child_page_num;
    void *right_high_key;

    right_child_page_num = InternalNodeGetRightNum(internal_node, default_value_len);
    right_high_key = InternalNodeGetRightKey(internal_node, default_value_len);
    SelectUnderCondition(oid, right_child_page_num, right_high_key, select_result, select_plan);

    /* The only condition to move to sibling:
     * The target node has spliten. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        uint32_t next_sibling = NodeGetNextSibling(table, internal_node);
        Assert(next_sibling != 0);
        SelectForInternalNode(oid, next_sibling, boundary_key, select_result, select_plan);
    }

    free_block(internal_node); 
}

/* The task for select internal node in async. */
static void SelectForInternalNodeChildTask(void *taskArg) {
    Assert(taskArg != NULL);

    SelectFromInternalChildTaskArgs *args = (SelectFromInternalChildTaskArgs *) taskArg;
    uint32_t child_page_num = args->page_num;
    SelectResult *select_result = args->select_result;
    Table *table = args->table;
    SelectPlan *select_plan = args->select_plan;

    Oid oid;
    Buffer child_buffer;
    void *child_node;

    oid = GET_TABLE_OID(table);
    child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
    child_node = GetBufferPage(child_buffer);

    switch (GetNodeType(child_node)) {
        case LEAF_NODE:
            SelectForLeafNode(oid, child_page_num, NULL, select_result, select_plan);
            break;
        case INTERNAL_NODE:
            SelectForInternalNode(oid, child_page_num, NULL, select_result, select_plan);
            break;
        default:
            db_log(PANIC, "Unknown node type.");
            break;
    }

    /* Release the child buffer. */
    ReleaseBuffer(child_buffer);
}

/* Select through internal node. */
static void SelectForInternalNodeAsync(Oid oid, uint32_t page_num, SelectResult *select_result,  SelectPlan *select_plan) {
    /* If LimitClauseNode full, not continue. */
    if (LimitClauseIsFull(select_plan))
        return;

    Table *table;
    Buffer buffer;
    void *internal_node;
    MetaColumn *primary_meta_column;
    uint32_t key_len, value_len, keys_num;

    table = open_table_inner(oid);
    buffer = ReadBuffer(oid, page_num);
    internal_node = GetBufferPage(buffer);

    /* Get variables. */
    key_len = table->key_len;
    value_len = table->index_value_len;
    keys_num = InternalNodeGetKeysNum(internal_node, value_len);
    primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);

    /* Prepare the parallel computing task args. */
    uint32_t taskNum = 0;
    SelectFromInternalChildTaskArgs *taskArgs[keys_num + 1];
    SelectResult *selectResults[keys_num + 1];

    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        void *max_key = GetComparableValue(InternalNodeGetCellKey(internal_node, key_len, value_len, i), primary_meta_column->column_type); 
        void *min_key = (i == 0) 
                    ? NULL 
                    : GetComparableValue(InternalNodeGetCellKey(internal_node, key_len, value_len, i - 1), primary_meta_column->column_type);
        KeyValue *max_key_value = new_key_value(primary_meta_column->column_name, max_key, primary_meta_column->column_type, oid, primary_meta_column->type_oid);
        KeyValue *min_key_value = new_key_value(primary_meta_column->column_name, min_key, primary_meta_column->column_type, oid, primary_meta_column->type_oid);
        if (!InternalNodeForSearchCondition(select_plan, min_key_value, max_key_value, select_plan->condition))
            continue;
        
        uint32_t child_page_num = InternalNodeGetCellValue(internal_node, key_len, value_len, i);
        selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name, true);
        taskArgs[taskNum] = instance(SelectFromInternalChildTaskArgs);
        taskArgs[taskNum]->select_result = selectResults[taskNum];
        taskArgs[taskNum]->page_num = child_page_num;
        taskArgs[taskNum]->keys_num = keys_num;
        taskArgs[taskNum]->table = table;
        taskArgs[taskNum]->select_plan = select_plan;
        taskNum++;
    }
   
    /* Don`t forget the right child. */
    uint32_t right_child_page_num = InternalNodeGetRightNum(internal_node, value_len);
    selectResults[taskNum] = new_select_result(SELECT_STMT, table->meta_table->table_name, true);
    taskArgs[taskNum] = instance(SelectFromInternalChildTaskArgs);
    taskArgs[taskNum]->select_result = selectResults[taskNum];
    taskArgs[taskNum]->page_num = right_child_page_num;
    taskArgs[taskNum]->keys_num = keys_num;
    taskArgs[taskNum]->table = table;
    taskArgs[taskNum]->select_plan = select_plan;
    taskNum++;

    /* Parallel compute. */
    ParallelCompute(4, taskNum, SelectForInternalNodeChildTask, (void **)taskArgs);

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

/* Select under condition. */
static void SelectUnderCondition(Oid oid, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Buffer buffer;
    void *node;
    NodeType type;
    
    buffer = ReadBuffer(oid, page_num); 
    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    type = GetNodeType(node);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    switch (type) {
        case LEAF_NODE: {
            if (select_plan->onlyScanIndex)
                ScanForLeafNode(oid, page_num, boundary_key, select_result, select_plan);
            else
                SelectForLeafNode(oid, page_num, boundary_key, select_result, select_plan);
            break;
        }
        case INTERNAL_NODE: {
            if (AsyncCondition(select_result)) 
                SelectForInternalNodeAsync(oid, page_num, select_result, select_plan);
            else
                SelectForInternalNode(oid, page_num, boundary_key, select_result, select_plan);
            break;
        }
        default:
            db_log(PANIC, "Unknown data type occurs in <query_with_condition>.");
    }

}

/* Query with condition inner. */
void QueryUnderSearchConditionInner(Oid oid, SelectResult *select_result, SelectPlan *select_plan) {
    SelectUnderCondition(oid, ROOT_PAGE_NUM, NULL, select_result, select_plan);
}

/* Query with condition. */
void QueryUnderSearchCondition(SelectResult *select_result, SelectPlan *select_plan) {
    /* Check if table exists. */
    Table *table = open_table(select_result->table_name);
    if (table == NULL) {
        db_log(ERROR, "Table %s not exist.", select_result->table_name);
        return;
    }
    QueryUnderSearchConditionInner(GET_TABLE_OID(table), select_result, select_plan);
}

/* Combine AtomNode by column and value. */
static AtomNode *GenerateAtomNode(MetaColumn *meta_column, void *value) {
    AtomNode *atom_node = instance(AtomNode);
    switch (meta_column->column_type) {
        case T_BOOL: {
            atom_node->type = A_BOOL;
            atom_node->value.boolval =  *(bool *)value;  
            break;
        }
        case T_CHAR: 
        case T_STRING:
        case T_DATE:
        case T_TIMESTAMP:
        case T_VARCHAR: {
            atom_node->type = A_STRING;
            atom_node->value.strval = value;  
            break;
        }
        case T_INT: 
        case T_LONG: {
            atom_node->type = A_INT;
            atom_node->value.intval = *(int64_t *) value;  
            break;
        }
        case T_DOUBLE:
        case T_FLOAT: {
            atom_node->type = A_INT;
            atom_node->value.floatval = *(double *) value;  
            break;
        }
        case T_RID:
        case T_OBJECT:
        case T_UNKNOWN:
            panic("Cant convert type to AtomNode.");
        break;
    }   
    return atom_node;
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
    predicate->comparison->left = instance(ScalarExpNode);
    predicate->comparison->left->type = SCALAR_COLUMN;
    predicate->comparison->left->column = instance(ColumnNode);
    predicate->comparison->left->column->column_name = dstrdup(meta_column->column_name);
    predicate->comparison->right = instance(ScalarExpNode);
    predicate->comparison->right->type = SCALAR_VALUE;
    predicate->comparison->right->value = instance(ValueItemNode);
    predicate->comparison->right->value->type = V_ATOM;
    predicate->comparison->right->value->value.atom = GenerateAtomNode(meta_column, value);

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
    QueryUnderSearchConditionInner(oid, result, SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, condtion));
    return result;
}

/* Count number of row, used in the sql function count(1) */
void CountRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    if (type == ARG_SELECT_PARAM && ((SelectPlan *) arg)->limitClause != NULL) {
        SelectPlan *select_plan = (SelectPlan *) arg;
        LimitClauseNode *limit_clause = select_plan->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (select_plan->offset >= limit_clause->offset && 
                select_plan->offset < (limit_clause->offset + limit_clause->rows)) {

            acquire_spin_lock(&select_plan->slock);
            /* Double check for concurrency. */
            if (select_plan->offset >= limit_clause->offset && 
                    select_plan->offset < (limit_clause->offset + limit_clause->rows)) {
                select_result->row_size++;
            } 
            release_spin_lock(&select_plan->slock);
        }

        __sync_fetch_and_add(&select_plan->offset, 1);
    } 
    else {
        select_result->row_size++;
    }
}

/* Select tuple data. */
void SelectTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectPlan *) arg)->limitClause != NULL) {
        SelectPlan *select_plan = (SelectPlan *) arg;
        LimitClauseNode *limit_clause = select_plan->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (select_plan->offset >= limit_clause->offset && 
                select_plan->offset < (limit_clause->offset + limit_clause->rows)) {
            acquire_spin_lock(&select_plan->slock);
            /* Double check for concurrency. */
            if (select_plan->offset >= limit_clause->offset && 
                    select_plan->offset < (limit_clause->offset + limit_clause->rows)) {
                AppendQueue(select_result->tuples, tuple);
                select_result->row_size++;
            }
            release_spin_lock(&select_plan->slock);
        }
        __sync_fetch_and_add(&select_plan->offset, 1);
    } else {
        AppendQueue(select_result->tuples, tuple);
        select_result->row_size++;
    }
}

/* Select row data. */
void SelectRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Row *row = GenerateRowInner(tuple, select_result->columns);
    /* If has limit clause. */
    if (type == ARG_SELECT_PARAM && ((SelectPlan *) arg)->limitClause != NULL) {
        SelectPlan *select_plan = (SelectPlan *) arg;
        LimitClauseNode *limit_clause = select_plan->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (select_plan->offset >= limit_clause->offset && 
                select_plan->offset < (limit_clause->offset + limit_clause->rows)) {
            acquire_spin_lock(&select_plan->slock);
            /* Double check for concurrency. */
            if (select_plan->offset >= limit_clause->offset && 
                    select_plan->offset < (limit_clause->offset + limit_clause->rows)) {
                AppendQueue(select_result->rows, row);
                select_result->row_size++;
            }
            release_spin_lock(&select_plan->slock);
        }
        __sync_fetch_and_add(&select_plan->offset, 1);
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
    if (type == ARG_SELECT_PARAM && ((SelectPlan *) arg)->limitClause != NULL) 
    {
        SelectPlan *select_plan = (SelectPlan *) arg;
        LimitClauseNode *limit_clause = select_plan->limitClause;

        /* If has limit clause, only append row whose pindex > offset and pindex < offset + rows. */
        if (select_plan->offset >= limit_clause->offset && 
                select_plan->offset < (limit_clause->offset + limit_clause->rows)) 
        {
            if (select_result->first_row_flag)
                select_result->first_row_flag = false;
            else
                db_send(", ");
            json_tuple(display_columns, tuple);
            select_result->row_size++;
        }
        __sync_fetch_and_add(&select_plan->offset, 1);
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
static KeyValue *CalcSumValue(ColumnNode *column, SelectResult *select_result, SelectPlan *select_plan) {
    double sum = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = QueryRowColumnValue(select_plan, column, row);
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
            case T_RID: 
            case T_OBJECT: {
                db_log(ERROR, "Reference type not used for sum function.");
                break;
            }
            default: {
                sum += 0;
                break;
            }
        }
    }
    return new_key_value(SUM_NAME, &sum, T_DOUBLE, select_result->oid, OID_ZERO);
}


/* Calulate column avg value. */
static KeyValue *CalcAvgValue(ColumnNode *column, SelectResult *select_result, SelectPlan *select_plan) {
    double sum = 0;
    double avg = 0;
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *key_value = QueryRowColumnValue(select_plan, column, row);
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
            case T_RID: 
            case T_OBJECT: {
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
    return new_key_value(AVG_NAME, &avg, T_DOUBLE, select_result->oid, OID_ZERO);
}


/* Calulate column max value.*/
static KeyValue *CalcMaxValue(ColumnNode *column, SelectResult *select_result, SelectPlan *select_plan) {
    void *max_value = NULL;
    DataType data_type = T_UNKNOWN;

    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = QueryRowColumnValue(select_plan, column, row);
        data_type = current->data_type;
        void *current_value = current->value;
        if (!max_value || GT(
                GetComparableValue(current_value, data_type), 
                GetComparableValue(max_value, data_type), 
                data_type)) 
        {
            if (max_value)
                free_value(max_value, data_type);
            max_value = copy_value(current_value, data_type);
        }
    }

    return new_key_value(MAX_NAME, max_value, data_type, select_result->oid, OID_ZERO);
}

/* Calulate column max value.*/
static KeyValue *CalcMinValue(ColumnNode *column, SelectResult *select_result, SelectPlan *select_plan) {
    void *min_value = NULL;
    DataType data_type = T_UNKNOWN;

    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        KeyValue *current = QueryRowColumnValue(select_plan, column, row);
        data_type = current->data_type;
        void *current_value = current->value;
        if (min_value == NULL || LT(
                GetComparableValue(current_value, data_type), 
                GetComparableValue(min_value, data_type), 
                data_type)
        ) {
            if (min_value)
                free_value(min_value, data_type);
            min_value = copy_value(current_value, data_type);
        }
    }

    return new_key_value(MIN_NAME, min_value, data_type, select_result->oid, OID_ZERO);
}


/* Query count function. */
static KeyValue *QueryCountFunctionValue(FunctionValueNode *value, SelectResult *select_result, SelectPlan *select_plan) {
    uint32_t row_size = select_result->row_size;
    return new_key_value(COUNT_NAME, &row_size, T_INT, select_result->oid, OID_ZERO);
}

/* Query sum function. */
static KeyValue *QuerySumFunctionValue(FunctionValueNode *value, SelectResult *select_result, SelectPlan *select_plan) {
    switch (value->value_type) {
        case V_COLUMN: 
            return CalcSumValue(value->column, select_result, select_plan);
        case V_INT: {
            double sum = value->i_value * (select_result->rows->size);
            return new_key_value(SUM_NAME, &sum, T_DOUBLE, select_result->oid, OID_ZERO);
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
KeyValue *QueryAvgFunctionValue(FunctionValueNode *value, SelectResult *select_result, SelectPlan *select_plan) {
    switch (value->value_type) {
        case V_COLUMN:
            return CalcAvgValue(value->column, select_result, select_plan);
        case V_INT: 
            return new_key_value(AVG_NAME, &value->i_value, T_DOUBLE, select_result->oid, OID_ZERO);
        case V_ALL: 
            db_log(ERROR, "Avg function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
}

/* Query max function. */
KeyValue *QueryMaxFunctionValue(FunctionValueNode *value, SelectResult *select_result, SelectPlan *select_plan) {
    switch (value->value_type) {
        case V_COLUMN: 
            return CalcMaxValue(value->column, select_result, select_plan);
        case V_INT: 
            return new_key_value(MAX_NAME, &value->i_value, T_INT, select_result->oid, OID_ZERO);
        case V_ALL: 
            db_log(ERROR, "Max function not support '*'.");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 

/* Query min function. */
KeyValue *QueryMinFunctionValue(FunctionValueNode *value, SelectResult *select_result, SelectPlan *select_plan) {
    switch (value->value_type) {
        case V_COLUMN:
            return CalcMinValue(value->column, select_result, select_plan);
        case V_INT: 
            return new_key_value(MIN_NAME, &value->i_value, T_INT, select_result->oid, OID_ZERO);
        case V_ALL: 
            db_log(PANIC, "Min function not support '*'");
            return NULL;
        default:
            UNEXPECTED_VALUE(value->value_type);
            return NULL;
    }
} 

/* Query scalar function */
static KeyValue *QueryFunctionColumnValue(FunctionNode *function, SelectResult *select_result, SelectPlan *select_plan) {
    switch (function->type) { 
        case F_COUNT:
            return QueryCountFunctionValue(function->value, select_result, select_plan);
        case F_SUM:
            return QuerySumFunctionValue(function->value, select_result, select_plan);
        case F_AVG:
            return QueryAvgFunctionValue(function->value, select_result, select_plan);
        case F_MAX:
            return QueryMaxFunctionValue(function->value, select_result, select_plan);
        case F_MIN:
            return QueryMinFunctionValue(function->value, select_result, select_plan);
        default:
            UNEXPECTED_VALUE("Not implement function yet.");
            return NULL;
    }
}

/* Query column value. */
static KeyValue *QueryRowColumnValue(SelectPlan *select_plan, ColumnNode *column, Row *row) {
    if (row == NULL) 
        return new_key_value(column->column_name, NULL, T_OBJECT, OID_ZERO, OID_ZERO);

    /* Get table name via alias name. */
    char *table_name = SearchTableNameViaAlias(select_plan, column->range_variable);
    if (column->range_variable && table_name == NULL) {
        db_log(ERROR, "Unknown table alias '%s' in select items. ", 
               column->range_variable);
        return NULL;
    }

    Table *table = StrIsEmpty(table_name) 
        ? NULL 
        : open_table(table_name);

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (StrEq(column->column_name, key_value->key) && 
            (table_name == NULL || GET_TABLE_OID(table) == key_value->tid)
        ) {
            /* Reference type and query sub column. */
            if (key_value->data_type == T_RID) {
                Rid ref_id = *(Rid *)key_value->value;
                Row *sub_row = DefineVisibleRow(key_value->type_id, ref_id);
                if (column->has_sub_column && column->sub_column) {
                    KeyValue *sub_key_value = QueryRowColumnValue(select_plan, column->sub_column, sub_row);
                    return sub_key_value;
                } else if (column->has_sub_column && column->scalar_exp_list) {
                    Row *filtered_subrow = QueryColumnsSelectOneRow(select_plan, column->scalar_exp_list, sub_row);
                    return new_key_value(column->column_name, filtered_subrow, T_OBJECT, key_value->type_id, OID_ZERO);
                } else if (!column->has_sub_column) {
                    return new_key_value(column->column_name, sub_row, T_OBJECT, key_value->type_id, OID_ZERO); 
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
                    return new_key_value(ADD_NAME, &sum, T_INT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t sum = *(int32_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sum = *(int32_t *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sum = *(int32_t *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sum = *(int64_t *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t sum = *(int64_t *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sum = *(int64_t *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sum = *(int64_t *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sum = *(float *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    float sum = *(float *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sum = *(float *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sum = *(float *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sum = *(double *)left->value + *(int32_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double sum = *(double *)left->value + *(int64_t *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double sum = *(double *)left->value + *(float *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sum = *(double *)left->value + *(double *)right->value;
                    return new_key_value(ADD_NAME, &sum, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(ADD_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(ADD_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
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
                    return new_key_value(SUB_NAME, &sub, T_INT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t sub = *(int32_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sub = *(int32_t *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sub = *(int32_t *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t sub = *(int64_t *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t sub = *(int64_t *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sub = *(int64_t *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sub = *(int64_t *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, OID_ZERO, OID_ZERO); 
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float sub = *(float *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    float sub = *(float *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float sub = *(float *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sub = *(float *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double sub = *(double *)left->value - *(int32_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double sub = *(double *)left->value - *(int64_t *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double sub = *(double *)left->value - *(float *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double sub = *(double *)left->value - *(double *)right->value;
                    return new_key_value(SUB_NAME, &sub, T_DOUBLE, OID_ZERO, OID_ZERO); 
                }
                default: {
                    int zero = 0;
                    return new_key_value(SUB_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(SUB_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
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
                    return new_key_value(MUL_NAME, &mul, T_INT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t mul = (*(int32_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float mul = (*(int32_t *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double mul = (*(int32_t *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    int64_t mul = (*(int64_t *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float mul = (*(int64_t *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul,  T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double mul = (*(int64_t *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_LONG, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_LONG, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    float mul = (*(float *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    float mul = (*(float *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    float mul = (*(float *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_FLOAT, OID_ZERO, OID_ZERO); 
                }
                case T_DOUBLE: {
                    double mul = (*(float *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double mul = (*(double *)left->value) * (*(int32_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double mul = (*(double *)left->value) * (*(int64_t *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double mul = (*(double *)left->value) * (*(float *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double mul = (*(double *)left->value) * (*(double *)right->value);
                    return new_key_value(MUL_NAME, &mul, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(MUL_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(MUL_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
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
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double div = (double)(*(int32_t *)left->value) / (*(int64_t *)right->value);
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double div = (double)(*(int32_t *)left->value) / (*(float *)right->value);
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double div = (double)(*(int32_t *)left->value) / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_LONG: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(int64_t *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double div = (double)*(int64_t *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double div = (double)*(int64_t *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double div = (double)*(int64_t *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_FLOAT: {
            switch (right->data_type) {
                case T_INT: {
                    double div = (double)*(float *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double div = (double)*(float *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double div = (double)*(float *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double div = (double)*(float *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        case T_DOUBLE: {
            switch (right->data_type) {
                case T_INT: {
                    double div = *(double *)left->value / *(int32_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_LONG: {
                    double div = *(double *)left->value / *(int64_t *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_FLOAT: {
                    double div = *(double *)left->value / *(float *)right->value;
                    return new_key_value(DIV_NAME, &div,  T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                case T_DOUBLE: {
                    double div = *(double *)left->value / *(double *)right->value;
                    return new_key_value(DIV_NAME, &div, T_DOUBLE, OID_ZERO, OID_ZERO);
                }
                default: {
                    int zero = 0;
                    return new_key_value(DIV_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
                }
            }
            break;
        }
        default: {
            int zero = 0;
            return new_key_value(DIV_NAME, &zero, T_INT, OID_ZERO, OID_ZERO);
        }
    }
}

/* Query function calculate. */
static KeyValue *QueryFunctionCalcValue(CalculateNode *calculate, SelectResult *select_result, SelectPlan *select_plan) {
    KeyValue *left, *right;
    left = QueryFunctionValue(calculate->left, select_result, select_plan);
    right = QueryFunctionValue(calculate->right, select_result, select_plan);

    switch (calculate->type) {
        case CAL_ADD:
            return CalcAddition(left, right);
        case CAL_SUB:
            return CalcSubstraction(left, right);
        case CAL_MUL:
            return CalcMultplication(left, right);
        case CAL_DIV:
            return CalcDivision(left, right);
        default:
            UNEXPECTED_VALUE(calculate->type);
    }
}

/* Query column value. */
static KeyValue *QueryFunctionValue(ScalarExpNode *scalar_exp, SelectResult *select_result, SelectPlan *select_plan) {
    Table *table = open_table(select_result->table_name);
    switch (scalar_exp->type) {
        case SCALAR_COLUMN: {
            ColumnNode *column = scalar_exp->column;
            MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, column->column_name);
            if (QueueIsEmpty(select_result->rows)) {
                return new_key_value(
                    column->column_name, 
                    NULL, 
                    meta_column->column_type,
                    select_result->oid,
                    OID_ZERO
                );
            }
            else {
                /* Default, when query function and column data, 
                 * column only return first data. */
                return QueryRowColumnValue(
                    select_plan, 
                    column, 
                    qfirst(QueueHead(select_result->rows))
                );
            }
        }
        case SCALAR_FUNCTION:
            return QueryFunctionColumnValue(scalar_exp->function, select_result, select_plan);
        case SCALAR_CALCULATE:
            return QueryFunctionCalcValue(scalar_exp->calculate, select_result, select_plan);
        case SCALAR_VALUE: {
            ValueItemNode *value = scalar_exp->value;
            if (QueueIsEmpty(select_result->rows)) 
                return new_key_value(VALUE_NAME, NULL, AtomTypeConvertDataType(value->value.atom->type), OID_ZERO, OID_ZERO);
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
static void QueryFunctionSelection(List *scalar_exp_list, SelectResult *select_result, SelectPlan *select_plan) {
    Row *row = NewRow();

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = QueryFunctionValue(scalar_exp, select_result, select_plan);        
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
static KeyValue *QueryRowCalcValue(SelectPlan *select_plan, CalculateNode *calculate, Row *row) {
    KeyValue *left, *right;
    left = QueryRowValue(select_plan, calculate->left, row);
    right = QueryRowValue(select_plan, calculate->right, row);

    switch (calculate->type) {
        case CAL_ADD:
            return CalcAddition(left, right);
        case CAL_SUB:
            return CalcSubstraction(left, right);
        case CAL_MUL:
            return CalcMultplication(left, right);
        case CAL_DIV:
            return CalcDivision(left, right);
        default:
            UNEXPECTED_VALUE(calculate->type);
    }
}

/* Query value item in scalar_exp. */
static KeyValue *QueryRowValueItem(ValueItemNode *value_item, Row *row) {
    Assert(value_item->type == V_ATOM);
    AtomNode *atom_node = value_item->value.atom;
    switch (atom_node->type) {
        case A_INT:
            return new_key_value(VALUE_NAME, &atom_node->value.intval, T_LONG, OID_ZERO, OID_ZERO);
        case A_BOOL:
            return new_key_value(VALUE_NAME, &atom_node->value.boolval, T_BOOL, OID_ZERO, OID_ZERO);
        case A_FLOAT:
            return new_key_value(VALUE_NAME, &atom_node->value.floatval, T_DOUBLE, OID_ZERO, OID_ZERO);
        case A_STRING:
            return new_key_value(VALUE_NAME, atom_node->value.strval, T_STRING, OID_ZERO, OID_ZERO);
        case A_REFERENCE:
            return new_key_value(VALUE_NAME, MakeEmptyRefer(), T_STRING, OID_ZERO, OID_ZERO);
        default:
            UNEXPECTED_VALUE(atom_node->type);
            return NULL;
    }
}

/* Query row value. */
static KeyValue *QueryRowValue(SelectPlan *select_plan, ScalarExpNode *scalar_exp, Row *row) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return QueryRowColumnValue(select_plan, scalar_exp->column, row);
        case SCALAR_CALCULATE:
            return QueryRowCalcValue(select_plan, scalar_exp->calculate, row);            
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
static Row *QueryColumnsSelectOneRow(SelectPlan *select_plan, List *scalar_exp_list, Row *row) {
    if (IsNull(row)) 
        return NULL;
    
    Row *sub_row = NewRow();

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        KeyValue *key_value = QueryRowValue(select_plan, scalar_exp, row);
        if (scalar_exp->alias) {
            /* Rename as alias. */
            key_value->key = dstrdup(scalar_exp->alias);
        }
        append_list(sub_row->data, key_value);
    }
    return sub_row;
}

/* Query all columns data. */
static void QueryColumnsSelection(List *scalar_exp_list, SelectResult *select_result, SelectPlan *select_plan) {
    QueueCell *qc;
    qforeach (qc, select_result->rows) {
        Row *row = qfirst(qc);
        qfirst(qc) = QueryColumnsSelectOneRow(select_plan, scalar_exp_list, row);
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
static void QueryWithSelection(SelectionNode *selection, SelectResult *select_result, SelectPlan *select_plan) {
    /* For all column, data has stream out by <query_row>*/
    if (selection->all_column)
        return;
    if (FunctionScalarExpExists(selection->scalar_exp_list)) 
        QueryFunctionSelection(selection->scalar_exp_list, select_result, select_plan);
    else 
        QueryColumnsSelection(selection->scalar_exp_list, select_result, select_plan);
}


/* Do before query condition. */
static void DoBeforeQuerySeachCondition(SelectPlan *select_plan) {
    if (select_plan->onlyAll && select_plan->stmt_type == SELECT_STMT) {
        MakeTempData("{ \"success\": true, \"data\": [");
    }
}

/* Do after query condition. */
static void DoAfterQuerySearchCondition(SelectPlan *select_plan, SelectResult *selectResult, DBResult *dbresult) {
    if (select_plan->onlyAll && select_plan->stmt_type == SELECT_STMT) {
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
static SelectResult *QueryMultiTableUnderSearchCondition(SelectNode *select_node, DBResult *dbresult, SelectPlan *select_plan) {
    List *table_list;
    SelectResult *head, *pres;

    /* If no from clause, return an empty select result. */
    if (IsNull(select_node->table_exp->from_clause)) 
        return new_select_result(SELECT_STMT, NULL, true);

    table_list = select_node->table_exp->from_clause->from;
    Assert(len_list(table_list) > 0);
    head = NULL;
    pres = NULL;

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
    DoBeforeQuerySeachCondition(select_plan);

    /* Do query condition. */
    QueryUnderSearchCondition(head, select_plan);

    /* Do after query condition. */
    DoAfterQuerySearchCondition(select_plan, head, dbresult);

    return head;
}

/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result) {
    /* Check SelectNode valid. */
    CheckForSelect(select_node);
    
    /* Generate select plan. */
    SelectPlan *select_plan = OptimizeSelect(select_node, result->stmt_type);

    /* Query multiple table with conditon and get select result which is after row filtered. */
    SelectResult *select_result = QueryMultiTableUnderSearchCondition(select_node, result, select_plan);

    /* Query Selection to define row content. */
    QueryWithSelection(select_node->selection, select_result, select_plan);

    /* If select all, return all row data. */
    result->rows = select_result->row_size;
    result->data = select_result;
    result->success = true;
    result->message = FormatStr("Query %d rows data from table '%s' successfully.", 
                                result->rows, 
                                select_result->table_name);

    /* Make up success result. */
    db_log(SUCCESS, "Query %d rows data from table '%s' successfully.", 
           result->rows, 
           select_result->table_name);
}
