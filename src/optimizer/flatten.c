#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include "flatten.h"
#include "mmgr.h"
#include "data.h"
#include "optimizer.h"

#define CANVAS_MAX_HEIGHT 1024
#define CANVAS_MAX_WIDTH 1024

/************* Make all type ExprNode **************/
static ExprNode *MakeAndExprNode(ExprNode *left, ExprNode *right) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_AND;
    expr->leftChild = left;
    expr->rightChild = right;
    return expr;
}

static ExprNode *MakeOrExprNode(ExprNode *left, ExprNode *right) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_OR;
    expr->leftChild = left;
    expr->rightChild = right;
    return expr;
}

static ExprNode *MakeNotExprNode(ExprNode *left) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_NOT;
    expr->leftChild = left;
    return expr;
}

static ExprNode *MakeVarExprNode(OprType opr, void *leftVal, void *rightVal) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_VAR;
    expr->opr = opr;
    expr->leftVal = leftVal;
    expr->rightVal = rightVal;
    return expr;
}

static ExprNode *MakeAndSetExprNode() {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_AND_SET;
    expr->children = create_list(NODE_EXPR_NODE);
    return expr;
}


static ExprNode *MakeOrSetExprNode() {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_OR_SET;
    expr->children = create_list(NODE_EXPR_NODE);
    return expr;
}

ExprNode *MakeTruthValueExprNode(bool truthVal) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_TRUTH_VALUE;
    expr->truthVal = truthVal;
    return expr;
}

/* Expr paser for boolean predicate. */
static ExprNode *ExprParseForPredicateNode(PredicateNode *predicate) {
    switch (predicate->type) {
        case PRE_COMPARISON: {
            ComparisonNode *comparison = predicate->comparison;
            return MakeVarExprNode((OprType) comparison->type, comparison->left, comparison->right);
        }
        case PRE_LIKE: {
            LikeNode *like = predicate->like;
            return MakeVarExprNode(OP_LIKE, like->column, like->value);
        }
        case PRE_IN: {
            InNode *in = predicate->in;
            return MakeVarExprNode(OP_IN, in->column, in->value_list);
        }
        default:
            UNEXPECTED_VALUE(predicate->type);
            return NULL;
    }
}

/* Expr paser for boolean primary. */
static ExprNode *ExprParseForBooleanPrimaryNode(BooleanPrimaryNode *boolean_primary) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return ExprParseForPredicateNode(boolean_primary->predicate);       
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return ExprParse(boolean_primary->search_condition);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return NULL;
    }
}

/* Expr paser for boolean test. */
static ExprNode *ExprParseForBooleanTestNode(BooleanTestNode *boolean_test) {
    switch (boolean_test->type) {
        case NONE_TRUE_VALUE:
            return ExprParseForBooleanPrimaryNode(boolean_test->boolean_primary);
        case IS_TRUTH_VALUE:
            return boolean_test->truth_value 
                ? ExprParseForBooleanPrimaryNode(boolean_test->boolean_primary)
                : MakeNotExprNode(ExprParseForBooleanPrimaryNode(boolean_test->boolean_primary));
        case IS_NOT_TRUTH_VALUE: 
            return boolean_test->truth_value 
                ? MakeNotExprNode(ExprParseForBooleanPrimaryNode(boolean_test->boolean_primary))
                : ExprParseForBooleanPrimaryNode(boolean_test->boolean_primary);
        case DIRECT_TRUE_VALUE:
            return MakeTruthValueExprNode(boolean_test->truth_value);
        default:
            UNEXPECTED_VALUE(boolean_test->type);
            return NULL;
    }
}

/* Expr paser for boolean factor. */
static ExprNode *ExprParseForBooleanFactorNode(BooleanFactorNode *boolean_factor) {
    return boolean_factor->is_not
            ? MakeNotExprNode(ExprParseForBooleanTestNode(boolean_factor->boolean_test))
            : ExprParseForBooleanTestNode(boolean_factor->boolean_test);
}

/* Expr paser for boolean term. */
static ExprNode *ExprParseForBooleanTermNode(BooleanTermNode *boolean_term) {
    return boolean_term->and_boolean_term == NULL
            ? ExprParseForBooleanFactorNode(boolean_term->boolean_factor)
            : MakeAndExprNode(ExprParseForBooleanTermNode(boolean_term->and_boolean_term), 
                              ExprParseForBooleanFactorNode(boolean_term->boolean_factor));
}

/* Expr paser for search condition. */
ExprNode *ExprParse(SearchConditionNode *search_condition) {
    if (search_condition == NULL) return NULL;
    return search_condition->or_search_condition == NULL
            ? ExprParseForBooleanTermNode(search_condition->boolean_term)
            : MakeOrExprNode(ExprParse(search_condition->or_search_condition), 
                             ExprParseForBooleanTermNode(search_condition->boolean_term));
}

/* BNF transform. 
 * This function will do the called BNF transform.
 * For example, it will execute the fllowing expr transformation:
 * (A OR B) AND (C OR D) ==> (A AND C) OR (B AND C) OR (A AND D) OR (B AND D).
 * This transform is crucial for selection of access path.
 * */
ExprNode *BNFTransform(ExprNode *node) {
    if (node == NULL || node->type == EXPR_VAR || node->type == EXPR_TRUTH_VALUE) 
        return node;

    node->leftChild = BNFTransform(node->leftChild);
    node->rightChild = BNFTransform(node->rightChild);

    if (node->type == EXPR_AND) {
        if (node->leftChild->type == EXPR_OR) {
            ExprNode *L1 = node->leftChild->leftChild;
            ExprNode *L2 = node->leftChild->rightChild;
            ExprNode *R = node->rightChild;

            /* Convert (A OR B) AND C ==> (A AND C) OR (B AND C). */
            return MakeOrExprNode(BNFTransform(MakeAndExprNode(L1, R)), 
                                  BNFTransform(MakeAndExprNode(L2, R)));
        } else if (node->rightChild->type == EXPR_OR) {
            ExprNode *R1 = node->rightChild->leftChild;
            ExprNode *R2 = node->rightChild->rightChild;
            ExprNode *L = node->leftChild;

            /* Convert A AND (C OR D) ==> (A AND C) OR (A AND D). */
            return MakeOrExprNode(BNFTransform(MakeAndExprNode(L, R1)), 
                                  BNFTransform(MakeAndExprNode(L, R2)));
        }
    }

    return node;
}

/* Get negate op. */
static OprType NegateOprType(OprType op) {
    switch (op) {
        case OP_EQ: return OP_NE;
        case OP_NE: return OP_EQ;
        case OP_GT: return OP_LE;
        case OP_GE: return OP_LT;
        case OP_LT: return OP_GE;
        case OP_LE: return OP_GT;
        case OP_IN: return OP_NOT_IN;
        case OP_LIKE: return OP_NOT_LIKE;
        case OP_NOT_IN: return OP_IN;
        case OP_NOT_LIKE: return OP_LIKE;
        default: return -1;
    }
}

/* Negate.
 * This function will push <not opr> down to its child to get simple and unified expr.
 * And it is crucial for selection cheapest path in optimizer.
 * */
ExprNode *Negate(ExprNode *node) {
    if (node == NULL) return node;
    if (node->type == EXPR_NOT) {
        ExprNode *child = node->leftChild;
        switch (child->type) {
            case EXPR_NOT: {
                /* NOT (NOT A) ==> A. */
                return Negate(child->leftChild);
            }
            case EXPR_AND: {
                /* NOT (A AND B) ==> (NOT A) OR (NOT B). */
                return MakeOrExprNode(Negate(MakeNotExprNode(child->leftChild)),
                                      Negate(MakeNotExprNode(child->rightChild)));
            }
            case EXPR_OR: {
                /* NOT (A OR B) ==> (NOT A) AND (NOT B). */
                return MakeAndExprNode(Negate(MakeNotExprNode(child->leftChild)), 
                                       Negate(MakeNotExprNode(child->rightChild)));
            }
            case EXPR_VAR: {
                /* NOT (Variabile Condiiton) ==> Change the opr. */
                child->opr = NegateOprType(child->opr);
                Assert(child->opr != -1);
                return child;
            }
            case EXPR_TRUTH_VALUE: {
                /* Not (true | false) ===> (false | true). */
                child->truthVal = !child->truthVal;
                return child;
            }
            default:
                UNEXPECTED_VALUE(node->type);
                return NULL;
        }
    }

    node->leftChild = Negate(node->leftChild);
    node->rightChild = Negate(node->rightChild);
    return node;
}

/* Flatten for logic expr node. */
static void FlattenForLogicNode(ExprNode *parent, ExprNode *current, ExprType expected) {
    if (current == NULL) return;
    if (current->type == expected) {
        FlattenForLogicNode(parent, current->leftChild, expected);
        FlattenForLogicNode(parent, current->rightChild, expected);
    } else 
        append_list(parent->children, Flatten(current));
}

/* Flatten. 
 * This function will pull the same logic children, transform binary tree 
 * to multi-path tree and to reduce the height of the tree.
 * */
ExprNode *Flatten(ExprNode *root) {
    if (root == NULL || root->type == EXPR_VAR) return root;
    switch (root->type) {
        case EXPR_AND: {
            ExprNode *node = MakeAndSetExprNode();
            FlattenForLogicNode(node, root->leftChild, EXPR_AND);
            FlattenForLogicNode(node, root->rightChild, EXPR_AND);
            return node;
        }
        case EXPR_OR: {
            ExprNode *node = MakeOrSetExprNode();
            FlattenForLogicNode(node, root->leftChild, EXPR_OR);
            FlattenForLogicNode(node, root->rightChild, EXPR_OR);
            return node;
        }
        default:
            break;
    }
    return root;
}


/* Draw canvas. */
static void DrawCanvas(ExprNode *node, char canvas[CANVAS_MAX_HEIGHT][CANVAS_MAX_WIDTH], int row, int col, int distance) {
    if (node == NULL) return;

    char *name = GetExprNodeName(node);
    int len = strlen(name);
    for (int i = 0; i < len && (col + i) < CANVAS_MAX_WIDTH; i++) {
        canvas[row][col + i] = name[i];
    }

    if (node->leftChild) {
        canvas[row + 1][col - distance / 2] = '/';
        DrawCanvas(node->leftChild, canvas, row + 2, col - distance, distance / 2);
    }

    if (node->rightChild) {
        canvas[row + 1][col + len + distance / 2] = '\\';
        DrawCanvas(node->rightChild, canvas, row + 2, col + distance, distance / 2);
    }

    if (node->children) {
        int avg = distance / len_list(node->children);
        ListCell *lc;
        foreach (lc, node->children) {
            canvas[row + 1][col - distance + avg * __i] = '|';
            DrawCanvas((ExprNode *)lfirst(lc), canvas, row + 2, col - distance + avg * __i, avg);
        }
    }
}

/* Print the expr node tree. */
void ExprPrint(ExprNode *node) {
    int i;
    char canvas[CANVAS_MAX_HEIGHT][CANVAS_MAX_WIDTH];
    
    /* Initial canvas. */
    for (i = 0; i < CANVAS_MAX_HEIGHT; i++) {
        memset(canvas[i], ' ', CANVAS_MAX_WIDTH);
    }

    /* Draw canvas. */
    DrawCanvas(node, canvas, 0, CANVAS_MAX_WIDTH / 2, 64);

    /* Print out. */
    for (i = 0; i < CANVAS_MAX_HEIGHT; i++) {
        int last = CANVAS_MAX_WIDTH - 1;
        while (last > 0 && canvas[i][last] == ' ') last--;
        if (last > 0) {
            canvas[i][last + 1] = '\0';
            printf("%s\n", canvas[i]);
        }
    }
}
