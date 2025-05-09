#include "data.h"
#ifndef SELECT_H
#define SELECT_H

typedef enum ROW_HANDLER_ARG_TYPE {
    ARG_NULL = 1,
    ARG_SELECT_PARAM,
    ARG_ASSIGNMENT_LIST,
    ARG_REFER_UPDATE_ENTITY
} ROW_HANDLER_ARG_TYPE;


/* Function pointer about row handler */
typedef void (*ROW_HANDLER)(Row *, SelectResult *select_result, Table *table, ROW_HANDLER_ARG_TYPE type,void *arg);

typedef struct SelectParam {
    bool onlyCount;                 /* Only count int select statement. */
    int32_t offset;                 /* Current offset. */
    LimitClauseNode *limitClause;   /* LimitClauseNode. */
    ROW_HANDLER rowHanler;          /* Row Handler implements.*/
} SelectParam;

/* Count number of row, used in the sql function count() */
void count_row(Row *row, SelectResult *select_result, Table *table, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Select row data. */
void select_row(Row *row, SelectResult *select_result, Table *table, ROW_HANDLER_ARG_TYPE type, void *arg);


/* Define row by refer. 
 * Return row not matter if it is deleted.
 * */
Row *define_row(Refer *refer);

/* Define row by refer. 
 * Return undelted row, return NULL if deleted.
 * */
Row *define_visible_row(Refer *refer);

/* Query with column and value. */
SelectResult *select_with_column_value(Oid oid, MetaColumn *meta_column, void *value);

/* Query with condition inner. */
void query_with_condition_inner(Oid oid, ConditionNode *condition, SelectResult *select_result, 
                                ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Query with condition. */
void query_with_condition(ConditionNode *condition, SelectResult *select_result, 
                          ROW_HANDLER row_handler, ROW_HANDLER_ARG_TYPE type, void *arg);


/* Execute select statement. */
void exec_select_statement(SelectNode *select_node, DBResult *result);

#endif
