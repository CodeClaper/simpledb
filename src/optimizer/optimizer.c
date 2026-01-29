#include <stdbool.h>
#include <string.h>
#include <time.h>
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
static MetaIndex *FindHitIndex(List *select_table_list, ExprNode *node);
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
    select_plan->hitIndex = FindHitIndex(select_plan->selectTableList, select_plan->condition_expr);
    select_plan->indexValid = select_plan->hitIndex != NULL;
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

static char *GetLikeStrValue(ValueItemNode *value_item) {
    Assert(value_item->type == V_ATOM);
    Assert(value_item->value.atom->type == A_STRING);
    return value_item->value.atom->value.strval;
}

/* Find hid index for column. */
static MetaIndex *FindHitIndexForColumn(List *select_table_list, ColumnNode *column) {
    Table *table;

    table = ColumnNodeFindTable(select_table_list, column);
    if (table == NULL) {
        Assert(len_list(select_table_list) == 1);
        SelectTable *first = (SelectTable *)lfirst(first_cell(select_table_list));
        table = first->table;
    }

    return ColumnNodeFindMetaIndex(table->meta_indexs, column);
}


/* Find hit index for scalar_exp. 
 * To be simple, we just think index invalid when scalar_exp is SCALAR_VALUE, SCALAR_CALCULATE, SCALAR_FUNCTION.
 * This logic need to be re-evaluated.
 * */
static MetaIndex *FindHitIndexForScalarExp(List *select_table_list, ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return FindHitIndexForColumn(select_table_list, scalar_exp->column);
        case SCALAR_VALUE:
        case SCALAR_CALCULATE:
        case SCALAR_FUNCTION:
            return NULL;
        default:
            UNEXPECTED_VALUE(scalar_exp->type);     
    }
}

/* Find the most efficient index when case EXPR_OR_SET. 
 * The algorithm is to find the biggest-size index.
 * */
static MetaIndex *FindEffcientIndexCaseExprOrSet(List *meta_indexs) {
    if (list_empty(meta_indexs)) return NULL;
    
    int page_size = 0;
    MetaIndex *hitIndex = NULL;

    ListCell *lc;
    foreach(lc, meta_indexs) {
        MetaIndex *current = (MetaIndex *) lfirst(lc);
        if (current->page_num > page_size) {
            page_size = current->page_num;
            hitIndex = current;
        }
    }

    return hitIndex;
}

/* Find hit index for comparison. 
 * To be simple, we just find hit index in the left value firstly, 
 * if not found, then find in the right value.
 * This logic need to be re-evaluated.
 * */
static MetaIndex *FindHitIndexForComparisonPredicate(List *select_table_list, ExprNode *node) {
    Assert(node->type == EXPR_VAR);
    MetaIndex *hitInex = FindHitIndexForScalarExp(select_table_list, node->leftVal);
    return hitInex != NULL 
            ? hitInex
            : FindHitIndexForScalarExp(select_table_list, node->rightVal);
}

/* Find hit index for in */
static MetaIndex *FindHitIndexForInPredicate(List *select_table_list, ExprNode *node) {
    return FindHitIndexForColumn(select_table_list, node->leftVal);
}

/* Find hit index for like. 
 * Index is valid when match right wildcard .*/
static MetaIndex *FindHitIndexForLikePredicate(List *select_table_list, ExprNode *node) {
    return NoneLeftWildcard(GetLikeStrValue(node->rightVal)) 
        ? FindHitIndexForColumn(select_table_list, node->leftVal) 
        : NULL;
}

/* Find hit index when case EXPR_VAR. */
static MetaIndex *FindHitIndexCaseExprVar(List *select_table_list, ExprNode *node) {
    switch (node->opr) {
        case OP_EQ: 
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE:
            return FindHitIndexForComparisonPredicate(select_table_list, node);
        case OP_LIKE:
        case OP_NOT_LIKE:
            return FindHitIndexForLikePredicate(select_table_list, node);
        case OP_IN:
        case OP_NOT_IN:
            return FindHitIndexForInPredicate(select_table_list, node);
        default:
            UNEXPECTED_VALUE(node->type);
            return NULL;
    }
}

/* Find hit index when case EXPR_AND_SET. 
 * To be simple, we just return the first hit index in children. 
 * */
static MetaIndex *FindHitIndexCaseExprAndSet(List *select_table_list, ExprNode *node) {
    Assert(node->type == EXPR_AND_SET);

    MetaIndex *hitIndex;
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        hitIndex = FindHitIndex(select_table_list, child);
        if (hitIndex != NULL) return hitIndex;
    }

    return NULL;
}

/* Find hit index when case EXPR_OR_SET. */
static MetaIndex *FindHitIndexCaseExprOrSet(List *select_table_list, ExprNode *node) {
    Assert(node->type == EXPR_OR_SET);
    List *meta_indexs = create_list(NODE_META_INDEX);

    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        MetaIndex *hitIndex = FindHitIndex(select_table_list, child);
        if (hitIndex != NULL) append_list(meta_indexs, hitIndex);
    }

    return FindEffcientIndexCaseExprOrSet(meta_indexs);
}


/* Find the hit index . */
static MetaIndex *FindHitIndex(List *select_table_list, ExprNode *node) {
    if (node == NULL) return NULL;
    switch (node->type) {
        case EXPR_VAR: 
            return FindHitIndexCaseExprVar(select_table_list, node);
        case EXPR_AND_SET:
            return FindHitIndexCaseExprAndSet(select_table_list, node);
        case EXPR_OR_SET:
            return FindHitIndexCaseExprOrSet(select_table_list, node);
        case EXPR_TRUTH_VALUE: 
            return NULL;
        default:
            UNEXPECTED_VALUE(node->type);
            return NULL;
    }
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
    if (search_condition == NULL) return NULL;
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
