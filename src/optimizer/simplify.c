#include "flatten.h"

/* Simplify. */
ExprNode *Simplify(ExprNode *root) {
    switch (root->type) {
        case EXPR_AND_SET:
        case EXPR_OR_SET:
        default:
            return root;
    }
}

