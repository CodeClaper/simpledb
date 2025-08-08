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

typedef struct SelectParam {
    StatementType stmt_type;        /* StatementType. */
    bool onlyAll;                   /* Only select all. */
    bool onlyCount;                 /* Only count int select statement. */
    bool onlyScanIndex;             /* Only scan index. */
    volatile int32_t offset;        /* Current offset. Need volatile in parall calculating.*/
    LimitClauseNode *limitClause;   /* LimitClauseNode. */
    ROW_HANDLER rowHanler;          /* Row Handler implements.*/
    s_lock slock;                   /* Sync lock.*/
} SelectParam;

/* Count number of row, used in the sql function count() */
void count_row(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Select tuple data. */
void select_tuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Select row data. */
void select_row(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Output tuple data. */
void output_tuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Define the tuple by refer. 
 * Return the tuple not matter if it is deleted and caller checks if deleted.
 * */
void *define_tuple(Refer *refer);

/* Define the row by refer. 
 * Return the row not matter if it is deleted and caller checks if deleted..
 * */
Row *define_row(Refer *refer);

/* Define row by refer. 
 * Return undelted row, return NULL if deleted.
 * */
Row *define_visible_row(Refer *refer);

/* Query with column and value. */
SelectResult *select_with_column_value(Oid oid, MetaColumn *meta_column, void *value);

/* Query with condition inner. */
void query_with_condition_inner(Oid oid, SearchConditionNode *condition, SelectResult *select_result, ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Query with condition. */
void query_with_condition(SearchConditionNode *condition, SelectResult *select_result, ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result);

#endif
