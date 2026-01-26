#include <stdbool.h>
#include <string.h>
#include "optimizer.h"
#include "data.h"
#include "mmgr.h"
#include "table.h"
#include "log.h"
#include "instance.h"
#include "flatten.h"
#include "simplify.h"
#include "select.h"

static bool OnlySelectAllInSelection(SelectNode *selectNode);
static bool OnlyCountInSelection(SelectNode *selectNode);
static bool OnlyScanIndex(SelectNode *selectNode);
static bool IndexValidForSelectNode(List *select_table_list, SelectNode *selectNode);
static bool IndexValidForSearchCondition(List *select_table_list, SearchConditionNode *search_condition);
static bool HitIndexForSelectNode(SelectPlan *select_plan, SelectNode *selectNode);
static bool HitIndexForSearchCondition(SelectPlan *select_plan, SearchConditionNode *search_condition);
static LimitClauseNode *SelectNodeFindLimitClause(SelectNode *selectNode);
static SearchConditionNode *SelectNodeFindCondition(SelectNode *selectNode);
static List *SelectNodeFindTables(SelectNode *selectNode);
static ROW_HANDLER DefineRowHandler(SelectPlan *select_plan);
static ExprNode *ConvertSearchConditionExpr(SearchConditionNode *search_condition);

/* Optimize Select Statment. */
SelectPlan *OptimizeSelect(SelectNode *selectNode, StatementType stmt_type) {
    SelectPlan *select_plan = instance(SelectPlan);
    select_plan->stmt_type = stmt_type;
    select_plan->condition = SelectNodeFindCondition(selectNode);
    select_plan->condition_expr = ConvertSearchConditionExpr(select_plan->condition);
    select_plan->selectTableList = SelectNodeFindTables(selectNode);
    select_plan->onlyAll = OnlySelectAllInSelection(selectNode);
    select_plan->onlyCount = OnlyCountInSelection(selectNode);
    select_plan->onlyScanIndex = OnlyScanIndex(selectNode);
    select_plan->indexValid = IndexValidForSelectNode(select_plan->selectTableList, selectNode);
    select_plan->hit_index = HitIndexForSelectNode(select_plan, selectNode);
    select_plan->limitClause = SelectNodeFindLimitClause(selectNode);
    select_plan->offset = 0;
    select_plan->rowHanler = DefineRowHandler(select_plan);
    select_plan->type = ARG_SELECT_PARAM;
    select_plan->arg = select_plan;
    init_spin_lock(&select_plan->slock);
    return select_plan;
}

/* Generate a Simple SelectPlan. */
SelectPlan *SimpleSelectPlan(ROW_HANDLER rowHanler, ROW_HANDLER_ARG_TYPE type, void *arg, SearchConditionNode *condition) {
    SelectPlan *select_plan = instance(SelectPlan);
    select_plan->condition = condition;
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

static bool ColumnNodeMatchMetaIndex(MetaIndex *meta_index, ColumnNode *column) {
    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (StrEq(current->column_name, column->column_name))
            return true;
    }

    return false;
}

static MetaIndex *ColumnNodeFindMetaIndex(List *meta_indexs, ColumnNode *column) {
    if (list_null_or_empty(meta_indexs)) 
        return NULL;

    ListCell *lc;
    foreach (lc, meta_indexs) {
        MetaIndex *current = (MetaIndex*) lfirst(lc);
        if (ColumnNodeMatchMetaIndex(current, column))
            return current;
    }

    return NULL;
}

static Table *ColumnNodeFindTable(List *select_table_list, ColumnNode *column) {
    if (StrIsEmpty(column->range_variable))
        return NULL;

    ListCell *lc;
    foreach (lc, select_table_list) {
        SelectTable *select_table = (SelectTable *)lfirst(lc);
        if (StrEq(select_table->alias_name, column->range_variable) || 
            StrEq(GET_TABLE_NAME(select_table->table), column->range_variable)
        ) return select_table->table;
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
        if (StrIsEmpty(column->range_variable))
            db_log(ERROR, "Unknown column '%s' in where clause. ", column->column_name);
        else
            db_log(ERROR, "Unknown column '%s.%s' in where clause. ", column->range_variable, column->column_name);
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

static bool HitIndexForColumn(SelectPlan *select_plan, ColumnNode *column) {
    Table *table;
    List *select_table_list;

    select_table_list = select_plan->selectTableList;
    table = ColumnNodeFindTable(select_table_list, column);
    if (table == NULL) {
        Assert(len_list(select_table_list) == 1);
        SelectTable *first = (SelectTable *)lfirst(first_cell(select_plan->selectTableList));
        table = first->table;
    }

    select_plan->meta_index = ColumnNodeFindMetaIndex(table->meta_indexs, column);
    return NonNull(select_plan->meta_index);
}


static bool HitIndexForScalarExp(SelectPlan *select_plan, ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return HitIndexForColumn(select_plan, scalar_exp->column);
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
static bool HitIndexForComparison(SelectPlan *select_plan, ComparisonNode *comparison) {
    return HitIndexForScalarExp(select_plan, comparison->left) && 
            HitIndexForScalarExp(select_plan, comparison->right);
}

static bool HitIndexForLike(SelectPlan *select_plan, LikeNode *like) {
    return HitIndexForColumn(select_plan, like->column) &&
            NoneLeftWildcard(like->value->value.atom->value.strval);
}

static bool HitIndexForIn(SelectPlan *select_plan, InNode *in) {
    return HitIndexForColumn(select_plan, in->column);
}

static bool HitIndexForPredicate(SelectPlan *select_plan, PredicateNode *predicate) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return HitIndexForComparison(select_plan, predicate->comparison);
        case PRE_LIKE:
            return HitIndexForLike(select_plan, predicate->like);
        case PRE_IN:
            return HitIndexForIn(select_plan, predicate->in);
        default:
            UNEXPECTED_VALUE(predicate->type);     
    }
}


static bool HitIndexForBooleanPrimary(SelectPlan *select_plan, BooleanPrimaryNode *boolean_primary) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return HitIndexForPredicate(select_plan, boolean_primary->predicate);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return HitIndexForSearchCondition(select_plan, boolean_primary->search_condition);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);     
    }
}

static bool HitIndexForBooleanTest(SelectPlan *select_plan, BooleanTestNode *boolean_test) {
    return HitIndexForBooleanPrimary(select_plan, boolean_test->boolean_primary);
}

/* If index valid for BooleanFactorNode. */
static bool HitIndexForBooleanFactor(SelectPlan *select_plan, BooleanFactorNode *boolean_factor) {
    return HitIndexForBooleanTest(select_plan, boolean_factor->boolean_test);
}

/* If index valid for BooleanTermNode. */
static bool HitIndexForBooleanTerm(SelectPlan *select_plan, BooleanTermNode *boolean_term) {
    return HitIndexForBooleanFactor(select_plan, boolean_term->boolean_factor) && 
            (boolean_term->and_boolean_term == NULL || HitIndexForBooleanTerm(select_plan, boolean_term->and_boolean_term));
}

static bool HitIndexForSearchCondition(SelectPlan *select_plan, SearchConditionNode *search_condition) {
    return HitIndexForBooleanTerm(select_plan, search_condition->boolean_term) && 
            (search_condition->or_search_condition == NULL || HitIndexForSearchCondition(select_plan, search_condition->or_search_condition));
}


/* Find the hit index if main invalid. */
static bool HitIndexForSelectNode(SelectPlan *select_plan, SelectNode *selectNode) {
    /* If main index hit, go main index. */
    if (select_plan->indexValid) return false;
    if (selectNode->table_exp == NULL || 
            selectNode->table_exp->where_clause == NULL || 
                selectNode->table_exp->where_clause->condition == NULL
    ) return false;

    SearchConditionNode *search_condition = selectNode->table_exp->where_clause->condition;
    return HitIndexForSearchCondition(select_plan, search_condition);
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

/* Get TableExpNode condition. 
 * If exists where clause, return its condition.
 * Else, return NULL. */
static SearchConditionNode *SelectNodeFindCondition(SelectNode *selectNode) {
    if (selectNode->table_exp == NULL || selectNode->table_exp->where_clause == NULL)
        return NULL;
    return selectNode->table_exp->where_clause->condition;
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

/* Convert search condtion to expr node. 
 * The basic routine:
 * Parse ==> Negate => BNF transfor ==> Flatten ==> Simplify.
 * */
static ExprNode *ConvertSearchConditionExpr(SearchConditionNode *search_condition) {
    return Simplify(
        Flatten(
            BNFTransform(
                Negate(
                    ExprParse(search_condition)
                )
            )
        )
    );
}

/* Get expr node name. */
char *GetExprNodeName(ExprNode *node) {
    switch (node->type) {
        case EXPR_AND: return dstrdup("AND");
        case EXPR_OR: return dstrdup("OR");
        case EXPR_NOT: return dstrdup("NOT");
        case EXPR_VAR: return dstrdup("VAR");
        case EXPR_AND_SET: return dstrdup("AND_SET");
        case EXPR_OR_SET: return dstrdup("OR_SET");
        case EXPR_TRUTH_VALUE: return dstrdup("TRUTH_VALUE");
        default:
            UNEXPECTED_VALUE(node->type);
            return NULL;
    }
}

/* Get op name. */
char *GetOprTypeName(OprType op) {
    switch (op) {
        case OP_EQ: return dstrdup("EQ");
        case OP_NE: return dstrdup("NE");
        case OP_GT: return dstrdup("GT");
        case OP_GE: return dstrdup("GE");
        case OP_LT: return dstrdup("LT");
        case OP_LE: return dstrdup("LE");
        case OP_IN: return dstrdup("IN");
        case OP_LIKE: return dstrdup("LIKE");
        case OP_NOT_IN: return dstrdup("NOT IN");
        case OP_NOT_LIKE: return dstrdup("NOT LIKE");
        default:
            UNEXPECTED_VALUE(op);
            return NULL;
    }
}
