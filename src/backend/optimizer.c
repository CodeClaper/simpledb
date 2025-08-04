#include <stdbool.h>
#include <string.h>
#include "select.h"
#include "mmgr.h"

static bool OnlySelectAllInSelection(SelectNode *selectNode);
static bool OnlyCountInSelection(SelectNode *selectNode);
static bool OnlyScanIndex(SelectNode *selectNode);
static LimitClauseNode *GetLimitClause(SelectNode *selectNode);
static ROW_HANDLER DefineRowHandler(SelectParam *selectParam);

/* Optimize Select Statment. */
SelectParam *optimizeSelect(SelectNode *selectNode, StatementType stmt_type) {
    SelectParam *selectParam = instance(SelectParam);
    selectParam->stmt_type = stmt_type;
    selectParam->onlyAll = OnlySelectAllInSelection(selectNode);
    selectParam->onlyCount = OnlyCountInSelection(selectNode);
    selectParam->onlyScanIndex = OnlyScanIndex(selectNode);
    selectParam->limitClause = GetLimitClause(selectNode);
    selectParam->offset = 0;
    selectParam->rowHanler = DefineRowHandler(selectParam);
    init_spin_lock(&selectParam->slock);
    return selectParam;
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

/* Get LimitClauseNode for the SelectionNode. */
static LimitClauseNode *GetLimitClause(SelectNode *selectNode) {
    if (selectNode->table_exp != NULL)
        return selectNode->table_exp->limit_clause;
    return NULL;
}

/* Define which ROW_HANDLER. 
 * (1) if only all column, use query_row.
 * (2) if only count, use count_row.
 * (3) otherwise, use select_row as default.
 * */
static ROW_HANDLER DefineRowHandler(SelectParam *selectParam) {
   return selectParam->onlyAll && selectParam->stmt_type == SELECT_STMT
            ? query_row
            : selectParam->onlyCount ? count_row : select_row;
}
