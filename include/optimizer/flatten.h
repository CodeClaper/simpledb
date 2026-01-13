#ifndef __FLATTEN_H__
#define __FLATTEN_H__

#include "data.h"

typedef enum { EXPR_AND, EXPR_OR, EXPR_NOT, EXPRE_VAR, EXPR_AND_SET, EXPR_OR_SET } ExprType;
typedef enum { OP_EQ, OP_NE, OP_GT, OP_GE, OP_LT, OP_LE, OP_LIKE, OP_IN } OprType;

typedef struct ExprNode {
    ExprType type;                      /* Which type. */
    OprType opr;                        /* For EXPRE_VAR*/
    void *leftVal;                      /* For EXPRE_VAR. */
    void *rightVal;                     /* For EXPRE_VAR. */
    struct ExprNode *leftChild;         /* For EXPR_AND, EXPR_OR. */
    struct ExprNode *rightChild;        /* For EXPR_AND, EXPR_OR. */
    List *children;                     /* For EXPR_AND_SET, EXPR_OR_SET.  */
} ExprNode;


ExprNode *ExprParse(SearchConditionNode *search_condition);
ExprNode *BNFTransform(ExprNode *node);
ExprNode *Flatten(ExprNode *expr);
void ExprPrint(ExprNode *node);

#endif
