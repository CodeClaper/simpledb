#include <stdbool.h>
#include <string.h>
#include "select.h"
#include "mmgr.h"

static bool OnlyCountInSelection(SelectNode *selectNode);
static LimitClauseNode *GetLimitClause(SelectNode *selectNode);

/* Optimize Select Statment. */
SelectParam *optimizeSelect(SelectNode *selectNode) {
    SelectParam *selectParam = instance(SelectParam);
    selectParam->onlyCount = OnlyCountInSelection(selectNode);
    selectParam->limitClause = GetLimitClause(selectNode);
    selectParam->offset = 0;
    selectParam->rowHanler = selectParam->onlyCount ? count_row : select_row;
    return selectParam;
}


/* Only Count function in the selection. 
 * Must safisfy:
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

/* Get LimitClauseNode for the SelectionNode. */
static LimitClauseNode *GetLimitClause(SelectNode *selectNode) {
    if (selectNode->table_exp != NULL)
        return selectNode->table_exp->limit_clause;
    return NULL;
}
