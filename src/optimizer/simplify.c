#include <stdbool.h>
#include "data.h"
#include "optimizer.h"

static bool FinalFalseCaseExprAndSet(ExprNode *node) {
    Assert(node->type == EXPR_AND_SET);
    return false;
}

static bool FinalTrueCaseExprOrSet(ExprNode *node) {
    Assert(node->type == EXPR_OR_SET);
    return false;
}

/* Simplify. */
ExprNode *Simplify(ExprNode *node) {
    switch (node->type) {
        case EXPR_AND_SET:
            node->final_false = FinalFalseCaseExprAndSet(node); 
            break;
        case EXPR_OR_SET: 
            node->final_true = FinalTrueCaseExprOrSet(node); 
            break;
        default: break;
    }
    return node;
}

