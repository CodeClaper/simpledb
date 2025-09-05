#include <stdbool.h>
#include "data.h"
#include "spinlock.h"
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
    StatementType stmt_type;        /* StatementType. */
    bool onlyAll;                   /* Only select all. */
    bool onlyCount;                 /* Only count int select statement. */
    bool onlyScanIndex;             /* Only scan index. */
    bool indexValid;                /* Index if valid. */
    SearchConditionNode *condition; /* The search condition. */
    List *selectTableList;          /* List of SelectTable. */
    volatile int32_t offset;        /* Current offset. Need volatile in parall calculating.*/
    LimitClauseNode *limitClause;   /* LimitClauseNode. */
    ROW_HANDLER rowHanler;          /* Row Handler implements.*/
    ROW_HANDLER_ARG_TYPE type;      /* Arguement type. */
    void *arg;                      /* Arguement. */
    s_lock slock;                   /* Sync lock.*/
} SelectPlan;

/* SelectFromInternalChildTaskArgs. */
typedef struct SelectFromInternalChildTaskArgs {
    SelectResult *select_result;
    uint32_t page_num;
    uint32_t keys_num;
    Table *table;
    SelectPlan *select_plan;
} SelectFromInternalChildTaskArgs;


/* Count number of row, used in the sql function count() */
void CountRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Select tuple data. */
void SelectTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Select row data. */
void SelectRow(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Output tuple data. */
void OutputTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Define the tuple by refer. 
 * Return the tuple not matter if it is deleted and caller checks if deleted. */
void *DefineTuple(Refer *refer);

/* Define the row by refer. 
 * Return the row not matter if it is deleted and caller checks if deleted.. */
Row *DefineRow(Refer *refer);

/* Define row by refer. 
 * Return undelted row, return NULL if deleted. */
Row *DefineVisibleRow(Refer *refer);

/* Query with column and value. */
SelectResult *SelectWithColumnValue(Oid oid, MetaColumn *meta_column, void *value);

/* Query with condition inner. */
void QueryUnderSearchConditionInner(Oid oid, SelectResult *select_result, SelectPlan *select_plan);

/* Query with condition. */
void QueryUnderSearchCondition(SelectResult *select_result, SelectPlan *select_plan);

/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result);

#endif
