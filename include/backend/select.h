#include <stdbool.h>
#include "data.h"
#include "spinlock.h"
#include "refer.h"

#ifndef SELECT_H
#define SELECT_H

typedef enum ROW_HANDLER_ARG_TYPE {
    ARG_NULL = 1,
    ARG_SELECT_PARAM,
    ARG_ASSIGNMENT_LIST,
    ARG_REFER_UPDATE_ENTITY
} ROW_HANDLER_ARG_TYPE;

/* Function pointer about row handler */
typedef void (*ROW_HANDLER)(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* SelectTable. */
typedef struct SelectTable {
    Table *table;
    char *alias_name;
} SelectTable;

/* Select Plan. */
typedef struct SelectPlan {
    StatementType stmt_type;            /* StatementType. */
    bool onlyAll;                       /* Only select all. */
    bool onlyCount;                     /* Only count int select statement. */
    bool onlyScanIndex;                 /* Only scan index. */
    bool indexValid;                    /* Index if valid. */
    bool hit_index;                     /* If hit index. */
    MetaIndex *meta_index;              /* The meta index if using index. */
    SearchConditionNode *condition;     /* The search condition. */
    List *selectTableList;              /* List of SelectTable. */
    volatile int32_t offset;            /* Current offset. Need volatile in parall calculating.*/
    LimitClauseNode *limitClause;       /* LimitClauseNode. */
    ROW_HANDLER rowHanler;              /* Row Handler implements.*/
    ROW_HANDLER_ARG_TYPE type;          /* Arguement type. */
    void *arg;                          /* Arguement. */
    s_lock slock;                       /* Sync lock.*/
} SelectPlan;

/* SelectFromInternalChildTaskArgs. */
typedef struct SelectFromInternalChildTaskArgs {
    SelectResult *select_result;
    uint32_t page_num;
    uint32_t keys_num;
    Table *table;
    SelectPlan *select_plan;
} SelectFromInternalChildTaskArgs;


void CountRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);
void SelectTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);
void SelectRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);
void OutputTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);
void *DefineTuple(Refer *refer);
Row *DefineVisibleRow(Oid toid, Rid ref_id);
bool LeafNodeForSearchCondition(SelectPlan *select_plan, List *meta_columns, void *tuple, SearchConditionNode *search_condition);
SelectResult *SelectWithColumnValue(Oid oid, MetaColumn *meta_column, void *value);
void QueryUnderSearchConditionInner(Oid oid, SelectResult *select_result, SelectPlan *select_plan);
void QueryUnderSearchCondition(SelectResult *select_result, SelectPlan *select_plan);
void exec_select_statement(SelectNode *select_node, DBResult *result);

#endif
