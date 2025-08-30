#include <stdbool.h>
#include <string.h>
#include "select.h"
#include "mmgr.h"
#include "table.h"
#include "log.h"
#include "instance.h"

static bool OnlySelectAllInSelection(SelectNode *selectNode);
static bool OnlyCountInSelection(SelectNode *selectNode);
static bool OnlyScanIndex(SelectNode *selectNode);
static bool IndexValidForSelectNode(List *select_table_list, SelectNode *selectNode);
static bool IndexValidForSearchCondition(List *select_table_list, SearchConditionNode *search_condition);
static LimitClauseNode *SelectNodeFindLimitClause(SelectNode *selectNode);
static List *SelectNodeFindTables(SelectNode *selectNode);
static ROW_HANDLER DefineRowHandler(SelectPlan *select_plan);

/* Optimize Select Statment. */
SelectPlan *OptimizeSelect(SelectNode *selectNode, StatementType stmt_type) {
    SelectPlan *select_plan = instance(SelectPlan);
    select_plan->stmt_type = stmt_type;
    select_plan->selectTableList = SelectNodeFindTables(selectNode);
    select_plan->onlyAll = OnlySelectAllInSelection(selectNode);
    select_plan->onlyCount = OnlyCountInSelection(selectNode);
    select_plan->onlyScanIndex = OnlyScanIndex(selectNode);
    select_plan->indexValid = IndexValidForSelectNode(select_plan->selectTableList, selectNode);
    select_plan->limitClause = SelectNodeFindLimitClause(selectNode);
    select_plan->offset = 0;
    select_plan->rowHanler = DefineRowHandler(select_plan);
    select_plan->type = ARG_SELECT_PARAM;
    select_plan->arg = select_plan;
    init_spin_lock(&select_plan->slock);
    return select_plan;
}

/* Generate a Simple SelectPlan. */
SelectPlan *SimpleSelectPlan(ROW_HANDLER rowHanler, ROW_HANDLER_ARG_TYPE type, void *arg) {
    SelectPlan *select_plan = instance(SelectPlan);
    select_plan->rowHanler = rowHanler;
    select_plan->type = type;
    select_plan->arg = arg;
    init_spin_lock(&select_plan->slock);
    return select_plan;
}

/* Only select all in selection.
 * Must satisfy:
 * (1) Only one scalarExp.
 * (2) The only one scalarExp is All column.
 * */
static bool OnlySelectAllInSelection(SelectNode *selectNode) {
    SelectionNode *selection;

    selection = selectNode->selection;
    Assert(selection != NULL);

    return selection->all_column;
}

/* Only Count function in the selection. 
 * Must satisfy:
 * (1) Only one scalarExp.
 * (2) The only one scalarExp is Count function.
 * */
static bool OnlyCountInSelection(SelectNode *selectNode) {
    SelectionNode *selection;
    List *scalarExpList;
    ScalarExpNode *scalarExp;

    selection = selectNode->selection;
    Assert(selection != NULL);
    if (selection->all_column)
        return false;

    scalarExpList = selection->scalar_exp_list;
    if (scalarExpList == NULL)
        return false;
    
    if (len_list(scalarExpList) != 1)
        return false;
    
    scalarExp = (ScalarExpNode *) lfirst(first_cell(scalarExpList));
    Assert(scalarExp != NULL);
    return scalarExp->type == SCALAR_FUNCTION && scalarExp->function->type == F_COUNT;
}

/* Only one table object. */
static bool OnlyOneFrom(FromClauseNode *from_clause) {
    return len_list(from_clause->from) == 1;
}

/* The conditon that satisfy only-scan-index. 
 * (1) Only count function in selection.
 * (2) No where conditon.
 * (3) Only one table object. */
static bool OnlyScanIndex(SelectNode *selectNode) {
    return OnlyCountInSelection(selectNode) && 
                selectNode->table_exp->where_clause == NULL && 
                    OnlyOneFrom(selectNode->table_exp->from_clause);
}

static MetaColumn *ColumnNodeFindMetaColumnInner(Table *table, ColumnNode *column) {
    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        if (StrEq(meta_column->column_name, column->column_name))
            return meta_column;
    }

    return NULL;
}


static MetaColumn *ColumnNodeFindMetaColumn(List *select_table_list, ColumnNode *column) {
    ListCell *lc;
    foreach (lc, select_table_list) {
        SelectTable *select_table = (SelectTable *) lfirst(lc);
        MetaColumn *target_meta_column = ColumnNodeFindMetaColumnInner(select_table->table, column);
        if (NonNull(target_meta_column))
            return target_meta_column;
    }

    return NULL;
}

static Table *ColumnNodeFindTable(List *select_table_list, ColumnNode *column) {
    if (StrIsEmpty(column->range_variable))
        return NULL;

    ListCell *lc;
    foreach (lc, select_table_list) {
        SelectTable *select_table = (SelectTable *)lfirst(lc);
        if (StrEq(select_table->alias_name, column->range_variable) || StrEq(GET_TABLE_NAME(select_table->table), column->range_variable))
            return select_table->table;
    }
    
    return NULL;
}   

/* Check if not left wildcard. */
static inline bool NoneLeftWildcard(char *wildcard) {
    return wildcard[0] != '%';
}

/* If index valid for ColumnNode. 
 * ------------------------------
 * Note: by now, we only support primary key, 
 * so if column is primary-key, of course index is valid.
 * */
static bool IndexValidForColumn(List *select_table_list, ColumnNode *column) {
    Table *table;
    MetaColumn *target_meta_column;

    table = ColumnNodeFindTable(select_table_list, column);
    target_meta_column = NonNull(table)
        ? ColumnNodeFindMetaColumnInner(table, column)
        : ColumnNodeFindMetaColumn(select_table_list, column);

    if (target_meta_column == NULL) {
        db_log(ERROR, "Unknown column '%s.%s' in where clause. ", 
               column->range_variable, 
               column->column_name);
    }
    return target_meta_column->is_primary;
}

/* If index valid for ScalarExpNode. 
 * --------------------------------
 * Note: Calucate or function maybe index-valid,
 * to be simple, they ars regarded as index-invaid.
 * */
static bool IndexValidForScalarExp(List *select_table_list, ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return IndexValidForColumn(select_table_list, scalar_exp->column);
        case SCALAR_VALUE:
            return true;
        case SCALAR_CALCULATE:
        case SCALAR_FUNCTION:
            return false;
        default:
            UNEXPECTED_VALUE(scalar_exp->type);     
    }
}

/* If index valid for ComparisonNode. */
static bool IndexValidForComparison(List *select_table_list, ComparisonNode *comparison) {
    return IndexValidForScalarExp(select_table_list, comparison->left) && 
            IndexValidForScalarExp(select_table_list, comparison->right);
}

/* If index valid for like predicate. 
 * ---------------------------------
 * Note: for like predicate, index works valid only for right wildcard,
 * full wildcard and left wildcard will cause index invalid. 
 * */
static bool IndexValidForLike(List *select_table_list, LikeNode *like) {
    return IndexValidForColumn(select_table_list, like->column) &&
            NoneLeftWildcard(like->value->value.atom->value.strval);
}

/* If index valid for in predicate. */
static bool IndexValidForIn(List *select_table_list, InNode *in) {
    return IndexValidForColumn(select_table_list, in->column);
}

/* If index valid for PredicateNode. */
static bool IndexValidForPredicate(List *select_table_list, PredicateNode *predicate) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return IndexValidForComparison(select_table_list, predicate->comparison);
        case PRE_LIKE:
            return IndexValidForLike(select_table_list, predicate->like);
        case PRE_IN:
            return IndexValidForIn(select_table_list, predicate->in);
        default:
            UNEXPECTED_VALUE(predicate->type);     
    }
}

/* If index valid for BooleanPrimaryNode. */
static bool IndexValidForBooleanPrimary(List *select_table_list, BooleanPrimaryNode *boolean_primary) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return IndexValidForPredicate(select_table_list, boolean_primary->predicate);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return IndexValidForSearchCondition(select_table_list, boolean_primary->search_condition);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);     
    }
}

/* If index valid for BooleanTestNode. */
static bool IndexValidForBooleanTest(List *select_table_list, BooleanTestNode *boolean_test) {
    return IndexValidForBooleanPrimary(select_table_list, boolean_test->boolean_primary);
}

/* If index valid for BooleanFactorNode. */
static bool IndexValidForBooleanFactor(List *select_table_list, BooleanFactorNode *boolean_factor) {
    return IndexValidForBooleanTest(select_table_list, boolean_factor->boolean_test);
}

/* If index valid for BooleanTermNode. */
static bool IndexValidForBooleanTerm(List *select_table_list, BooleanTermNode *boolean_term) {
    return IndexValidForBooleanFactor(select_table_list, boolean_term->boolean_factor) && 
            (boolean_term->and_boolean_term == NULL || IndexValidForBooleanTerm(select_table_list, boolean_term->and_boolean_term));
}

/* If index valid for SearchConditionNode. */
static bool IndexValidForSearchCondition(List *select_table_list, SearchConditionNode *search_condition) {
    return IndexValidForBooleanTerm(select_table_list, search_condition->boolean_term) && 
            (search_condition->or_search_condition == NULL || IndexValidForSearchCondition(select_table_list, search_condition->or_search_condition));
}

/* If index valid for search condition. */
static bool IndexValidForSelectNode(List *select_table_list, SelectNode *selectNode) {
    if (selectNode->table_exp == NULL || 
            selectNode->table_exp->where_clause == NULL || 
                selectNode->table_exp->where_clause->condition == NULL)
        return false;
    SearchConditionNode *search_condition = selectNode->table_exp->where_clause->condition;
    return IndexValidForSearchCondition(select_table_list, search_condition);
}

/* Get LimitClauseNode for the SelectionNode. */
static LimitClauseNode *SelectNodeFindLimitClause(SelectNode *selectNode) {
    return (selectNode->table_exp != NULL) ? selectNode->table_exp->limit_clause : NULL;
}

static List *SelectNodeFindTables(SelectNode *selectNode) {
    if (selectNode == NULL || selectNode->table_exp == NULL || selectNode->table_exp->from_clause == NULL)
        return NIL; 
    List *select_table_list;
    FromClauseNode *from_clause;
    
    select_table_list = create_list(NODE_VOID);
    from_clause = selectNode->table_exp->from_clause;

    ListCell *lc;
    foreach (lc, from_clause->from) {
        TableRefNode *table_ref = (TableRefNode *) lfirst(lc);
        Table *table = open_table(table_ref->table);
        if (table == NULL)
            db_log(ERROR, "Not found table %s", table_ref->range_variable);
        SelectTable *select_table = instance(SelectTable);
        select_table->table = table;
        select_table->alias_name = table_ref->range_variable;
        append_list(select_table_list, select_table);
    }

    return select_table_list;
}

/* Define which ROW_HANDLER. 
 * (1) if only all column, use <output_tuple>.
 * (2) if only count, use count_row.
 * (3) otherwise, use select_row as default.
 * */
static ROW_HANDLER DefineRowHandler(SelectPlan *select_plan) {
   return select_plan->onlyAll && select_plan->stmt_type == SELECT_STMT
            ? OutputTuple
            : select_plan->onlyCount ? CountRow : SelectRow;
}
