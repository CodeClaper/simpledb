#include <stdbool.h>
#include "simplify.h"
#include "data.h"
#include "flatten.h"
#include "optimizer.h"
#include "instance.h"
#include "meta.h"
#include "compare.h"
#include "log.h"

/* Scalar exp is calulable. 
 * (1) Scalar exp must be SCALAR_VALUE.
 * (2) Value item must be V_ATOM. 
 * (2) Atom must not be A_REFERENCE. */
static bool ScalarExpIsCalculable(ScalarExpNode *scalar_exp) {
    if (scalar_exp->type != SCALAR_VALUE) return false;
    if (scalar_exp->value->type != V_ATOM) return false;
    if (scalar_exp->value->value.atom->type == A_REFERENCE) return false;
    else return true;
}

/* Scalar exp get atom type. */
static inline AtomType ScalarExpGetAtomType(ScalarExpNode *scalar_exp) {
    return scalar_exp->value->value.atom->type;
}

static inline void *ScalarExpGetValue(ScalarExpNode *scalar_exp) {
    return ValueItemNodeFindValue(scalar_exp->value);
}

/* Simplify When case VAR. */
static ExprNode *SimplifyCaseVar(ExprNode *node) {
    switch (node->opr) {
        case OP_EQ: 
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE: {
            if (!ScalarExpIsCalculable(node->leftVal) || !ScalarExpIsCalculable(node->rightVal)) return node;
            KeyValue *leftKv = new_simple_key_value(NULL, ScalarExpGetValue(node->leftVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->leftVal)));
            KeyValue *rightKv = new_simple_key_value(NULL, ScalarExpGetValue(node->rightVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->rightVal)));
            bool ret = KeyValueEval((CompareType)node->opr, leftKv, rightKv);
            return MakeTruthValueExprNode(ret);
        }
        /* We just think <like> or <in> include column(actually. In fact, it`s wrong. 
         * For example, the sql "select * from tb where 'a' in ('a', 'b', 'c')" is executable. 
         * To make it simple, just return fasle. */
        case OP_LIKE:
        case OP_IN:
        case OP_NOT_LIKE:
        case OP_NOT_IN: return node;
        default: unreachable(node, "Not suport expr opr: %d", node->opr);
    }
}

/* Simplify when case EXPR_TRUTH_VALUE. */
static inline ExprNode *SimplifyCaseTruthVal(ExprNode *node) {
    return node;
}

/* Simplify when case EXPR_AND_SET. */
static ExprNode *SimplifyCaseExprAndSet(ExprNode *node) {
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_AND_SET node, children are only EXPR_VAR, EXPR_TRUTH_VALUE.  */
        switch (child->type) {
            case EXPR_VAR: {
                child = SimplifyCaseVar(child); 
                switch (child->type) {
                    case EXPR_VAR:
                        lfirst(lc) = child;
                        break;
                    case EXPR_TRUTH_VALUE:
                        if (child->truthVal) {
                            list_delete_cell(node->children, lc);
                            stepback();
                            break;
                        }
                        return MakeTruthValueExprNode(false);
                    default: unreachable(node, "Logic error.");
                }
                break;
            }
            case EXPR_TRUTH_VALUE: {
                if (child->truthVal) {
                    list_delete_cell(node->children, lc);
                    stepback();
                    break;
                }
                return MakeTruthValueExprNode(false);
            }
            default: unreachable(node, "Logic error.");
        }
    }
    return list_empty(node->children) ? MakeTruthValueExprNode(true) : node;
}

/* Simplify when case EXPR_OR_SET. */
static ExprNode *SimplifyCaseExprOrSet(ExprNode *node) {
    ListCell *lc;
    foreach (lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_OR_SET node, children just are EXPR_AND_SET or EXPR_VAR or EXPR_TRUTH_VALUE.  */
        switch (child->type) {
            case EXPR_AND_SET: {
                child = SimplifyCaseExprAndSet(child);
                switch (child->type) {
                    case EXPR_AND_SET: break;
                    case EXPR_TRUTH_VALUE: {
                        if (!child->truthVal) {
                            list_delete_cell(node->children, lc);
                            stepback();
                            break;
                        }
                        return MakeTruthValueExprNode(true);
                    }
                    default: unreachable(node, "Logic error.");
                }
                break;
            }
            case EXPR_VAR: {
                child = SimplifyCaseVar(child);
                switch (child->type) {
                    case EXPR_VAR:
                        lfirst(lc) = child;
                        break;
                    case EXPR_TRUTH_VALUE:
                        if (!child->truthVal) {
                            list_delete_cell(node->children, lc);
                            stepback();
                            break;
                        }
                        return MakeTruthValueExprNode(true);
                    default: unreachable(node, "Logic error.");
                }
                break;
            }
            case EXPR_TRUTH_VALUE: {
                if (!child->truthVal) {
                    list_delete_cell(node->children, lc);
                    stepback();
                    break;
                }
                return MakeTruthValueExprNode(true);
            }
            default: unreachable(node, "Logic error.");
        }
    }
    return list_empty(node->children) ? MakeTruthValueExprNode(false) : node;
}

/* Simplify. */
ExprNode *Simplify(ExprNode *node) {
    if (node == NULL) return NULL;
    switch (node->type) {
        case EXPR_VAR: return SimplifyCaseVar(node);
        case EXPR_TRUTH_VALUE: return SimplifyCaseTruthVal(node);
        case EXPR_AND_SET: return SimplifyCaseExprAndSet(node); 
        case EXPR_OR_SET: return SimplifyCaseExprOrSet(node); 
        default: unreachable(node, "Logic error");
    }
}
