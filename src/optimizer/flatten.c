#include "flatten.h"
#include "mmgr.h"
#include "data.h"

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
    expr->type = EXPRE_VAR;
    expr->opr = opr;
    expr->leftVal = leftVal;
    expr->rightVal = rightVal;
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
