#ifndef SELECT_H
#define SELECT_H

#include <stdbool.h>
#include "data.h"
#include "optimizer.h"
#include "spinlock.h"
#include "refer.h"

/* SelectFromInternalChildTaskArgs. */
typedef struct SelectFromInternalChildTaskArgs {
    SelectResult *select_result;
    uint32_t page_num;
    uint32_t keys_num;
    Table *table;
    SelectPlan *select_plan;
} SelectFromInternalChildTaskArgs;


void CountRow(void *tuple, SelectResult *select_result, SelectPlan *select_plan);
void SelectTuple(void *tuple, SelectResult *select_result, SelectPlan *select_plan);
void SelectRow(void *tuple, SelectResult *select_result, SelectPlan *select_plan);
void OutputTuple(void *tuple, SelectResult *select_result, SelectPlan *select_plan);
void *DefineTuple(Refer *refer);
Row *DefineVisibleRow(Oid toid, Rid ref_id);
bool LeafNodeForSearchCondition(SelectPlan *select_plan, List *meta_columns, void *tuple, SearchConditionNode *search_condition);
SelectResult *SelectWithColumnValue(Oid oid, MetaColumn *meta_column, void *value);
void QueryUnderSearchConditionInner(Oid oid, SelectResult *select_result, SelectPlan *select_plan);
void QueryUnderSearchCondition(SelectResult *select_result, SelectPlan *select_plan);
void exec_select_statement(SelectNode *select_node, DBResult *result);

#endif
