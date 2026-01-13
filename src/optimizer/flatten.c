#include <stdio.h>
#include <string.h>
#include "flatten.h"
#include "mmgr.h"
#include "data.h"

#define CANVAS_MAX_HEIGHT 1024
#define CANVAS_MAX_WIDTH 1024

static ExprNode *MakeAndExprNode(ExprNode *left, ExprNode *right) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_AND;
    expr->leftChild = left;
    expr->rightChild = right;
    expr->children = create_list(NODE_VOID);
    return expr;
}

static ExprNode *MakeOrExprNode(ExprNode *left, ExprNode *right) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_OR;
    expr->leftChild = left;
    expr->rightChild = right;
    expr->children = create_list(NODE_VOID);
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
    expr->type = EXPRE_VAR;
    expr->opr = opr;
    expr->leftVal = leftVal;
    expr->rightVal = rightVal;
    return expr;
}

static ExprNode *MakeAndSetExprNode(List *child) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_AND_SET;
    expr->children = child;
    return expr;
}


static ExprNode *MakeOrSetExprNode(List *child) {
    ExprNode *expr = instance(ExprNode);
    expr->type = EXPR_OR_SET;
    expr->children = child;
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
            : MakeAndExprNode(ExprParseForBooleanFactorNode(boolean_term->boolean_factor), ExprParseForBooleanTermNode(boolean_term->and_boolean_term));
}

/* Expr paser for search condition. */
ExprNode *ExprParse(SearchConditionNode *search_condition) {
    if (search_condition == NULL) return NULL;
    return search_condition->or_search_condition == NULL
            ? ExprParseForBooleanTermNode(search_condition->boolean_term)
            : MakeOrExprNode(ExprParseForBooleanTermNode(search_condition->boolean_term), ExprParse(search_condition->or_search_condition));
}

/* BNF transform. 
 * This function will do the called BNF transform.
 * For example, it will execute the fllowing expr:
 * (A OR B) AND (C OR D) ==> (A AND C) OR (B AND C) OR (A AND D) OR (B AND D).
 * This transform is crucial for selection of access path.
 * */
ExprNode *BNFTransform(ExprNode *node) {
    if (node == NULL || node->type == EXPRE_VAR) 
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

static void FlattenLogicNode(ExprNode *parent, ExprNode *current) {
    if (current == NULL) return;
    if (current->type == parent->type) {
        FlattenLogicNode(parent, current->leftChild);
        FlattenLogicNode(parent, current->rightChild);
    } else 
        append_list(parent->children, current);
}

/* Flatten. 
 * This function aims to transform binary tree to multi-path tree 
 * and to reduce the height of the tree.
 * */
ExprNode *Flatten(ExprNode *node) {
    if (node == NULL || node->type == EXPRE_VAR) return node;
    switch (node->type) {
        case EXPR_AND: {
            node->type = EXPR_AND_SET;
            FlattenLogicNode(node, node->leftChild);
            FlattenLogicNode(node, node->rightChild);
            break;
        }
        case EXPR_OR: {
            node->type = EXPR_OR_SET;
            FlattenLogicNode(node, node->leftChild);
            FlattenLogicNode(node, node->rightChild);
            break;
        }
        default:
            break;
    }
    return node;
}

static char* GetNodeName(ExprNode *node) {
    switch (node->type) {
        case EXPR_AND:
            return dstrdup("AND");
        case EXPR_OR:
            return dstrdup("OR");
        case EXPR_NOT:
            return dstrdup("NOT");
        case EXPRE_VAR:
            return dstrdup("VAR");
        case EXPR_AND_SET:
            return dstrdup("AND_SET");
        case EXPR_OR_SET:
            return dstrdup("OR_SET");
        default:
            UNEXPECTED_VALUE(node->type);
            return NULL;
    }
}

static void DrawExprNode(ExprNode *node, char canvas[CANVAS_MAX_HEIGHT][CANVAS_MAX_WIDTH], int row, int col, int distance) {
    if (node == NULL) return;

    char *name = GetNodeName(node);
    int len = strlen(name);
    for (int i = 0; i < len && (col + i) < CANVAS_MAX_WIDTH; i++) {
        canvas[row][col + i] = name[i];
    }

    if (node->leftChild) {
        canvas[row + 1][col - distance / 2] = '/';
        DrawExprNode(node->leftChild, canvas, row + 2, col - distance, distance / 2);
    }

    if (node->rightChild) {
        canvas[row + 1][col + len + distance / 2] = '\\';
        DrawExprNode(node->rightChild, canvas, row + 2, col + distance, distance / 2);
    }

    if (node->children) {
        int avg = distance / len_list(node->children);
        ListCell *lc;
        foreach (lc, node->children) {
            canvas[row + 1][col - distance + avg * __i] = '|';
            DrawExprNode((ExprNode *)lfirst(lc), canvas, row + 2, col - distance + avg * __i, avg);
        }
    }
}

void ExprPrint(ExprNode *node) {
    char canvas[CANVAS_MAX_HEIGHT][CANVAS_MAX_WIDTH];
    for (int i = 0; i < CANVAS_MAX_HEIGHT; i++) {
        memset(canvas[i], ' ', CANVAS_MAX_WIDTH);
    }

    DrawExprNode(node, canvas, 0, CANVAS_MAX_WIDTH / 2, 64);

    for (int i = 0; i < CANVAS_MAX_HEIGHT; i++) {
        int last = CANVAS_MAX_WIDTH - 1;
        while (last > 0 && canvas[i][last] == ' ') last--;
        if (last > 0) {
            canvas[i][last + 1] = '\0';
            printf("%s\n", canvas[i]);
        }
    }
}
