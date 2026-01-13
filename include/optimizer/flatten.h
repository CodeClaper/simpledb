#ifndef __FLATTEN_H__
#define __FLATTEN_H__

#include "data.h"

typedef enum { EXPR_AND, EXPR_OR, EXPR_NOT, EXPRE_VAR } ExprType;
typedef enum { OP_EQ, OP_NE, OP_GT, OP_GE, OP_LT, OP_LE, OP_LIKE, OP_IN } OprType;

typedef struct ExprNode {
    ExprType type;
    OprType opr;
    void *leftVal;
    void *rightVal;
    struct ExprNode *leftChild;
    struct ExprNode *rightChild;
} ExprNode;


ExprNode *ExprParse(SearchConditionNode *search_condition);

#endif
