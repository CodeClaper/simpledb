#ifndef __OPTIMIZER_H__
#define __OPTIMIZER_H__

#include <stdbool.h>
#include "data.h"
#include "spinlock.h"

typedef enum { EXPR_AND, EXPR_OR, EXPR_NOT, EXPR_VAR, EXPR_AND_SET, EXPR_OR_SET, EXPR_TRUTH_VALUE } ExprType;
typedef enum { OP_EQ, OP_NE, OP_GT, OP_GE, OP_LT, OP_LE, OP_LIKE, OP_IN, OP_NOT_LIKE, OP_NOT_IN } OprType;

typedef struct ExprNode {
    ExprType type;                      /* Which type. */
    OprType opr;                        /* For EXPR_VAR. */
    void *leftVal;                      /* For EXPR_VAR. */
    void *rightVal;                     /* For EXPR_VAR. */
    bool truthVal;                      /* For EXPR_TRUTH_VALUE. */
    struct ExprNode *leftChild;         /* For EXPR_AND, EXPR_OR. */
    struct ExprNode *rightChild;        /* For EXPR_AND, EXPR_OR. */
    List *children;                     /* For EXPR_AND_SET, EXPR_OR_SET.  */
    int sflag;                          /* Simplify flag. */
} ExprNode;


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
    MetaIndex *hitIndex;                 /* The hit index. */
    SearchConditionNode *condition;     /* The search condition. */
    ExprNode *condition_expr;           /* The condition expr. */                    
    List *selectTableList;              /* List of SelectTable. */
    volatile int32_t offset;            /* Current offset. Need volatile in parall calculating.*/
    LimitClauseNode *limitClause;       /* LimitClauseNode. */
    ROW_HANDLER rowHanler;              /* Row Handler implements.*/
    ROW_HANDLER_ARG_TYPE type;          /* Arguement type. */
    void *arg;                          /* Arguement. */
    s_lock slock;                       /* Sync lock.*/
} SelectPlan;

char *GetExprNodeName(ExprNode *node);
char *GetOprTypeName(OprType op);
bool MetaColumnMatchExprVar(List *select_table_list, MetaColumn *meta_column, ExprNode *node);
SelectPlan *OptimizeSelect(SelectNode *selectNode, StatementType stmt_type);
SelectPlan *SimpleSelectPlan(ROW_HANDLER rowHanler, ROW_HANDLER_ARG_TYPE type, void *arg, SearchConditionNode *condition);

#endif
