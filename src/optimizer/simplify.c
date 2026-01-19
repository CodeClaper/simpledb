#include <stdbool.h>
#include <time.h>
#include "simplify.h"
#include "data.h"
#include "optimizer.h"
#include "instance.h"
#include "meta.h"
#include "compare.h"

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

/* For var child, the final_true is true or not. */
static bool FinalFalseForVarChild(ExprNode *node) {
    switch (node->opr) {
        case OP_EQ: 
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE: {
            if (!ScalarExpIsCalculable(node->leftVal) || !ScalarExpIsCalculable(node->rightVal)) return false;
            KeyValue *leftKv = new_simple_key_value(NULL, ScalarExpGetValue(node->leftVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->leftVal)));
            KeyValue *rightKv = new_simple_key_value(NULL, ScalarExpGetValue(node->rightVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->rightVal)));
            return KeyValueEval((CompareType)node->opr, leftKv, rightKv);
        }
        /* We just think <like> or <in> include column(actually, it`s wrong, for example, 
         * the sql "select * from tb where 'a' in ('a', 'b', 'c')" is executable.), 
         * so just return fasle.*/
        case OP_LIKE:
        case OP_IN:
        case OP_NOT_LIKE:
        case OP_NOT_IN:
            return false;
        default:
            UNEXPECTED_VALUE(node->type);
            return false;
    }
}

static bool FinalFalseCaseExprAndSet(ExprNode *node) {
    Assert(node->type == EXPR_AND_SET);
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_AND_SET node, children just are EXPR_VAR.  */
        switch (child->type) {
            case EXPR_VAR: {
                if (!FinalFalseForVarChild(child)) return false;
                else break;
            }
            default:
                UNEXPECTED_VALUE(node->type);
        }
    }
    return true;
}

/* For var child, the final_true is true or not. */
static bool FinalTrueForVarChild(ExprNode *node) {
    switch (node->opr) {
        case OP_EQ: 
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE: {
            if (!ScalarExpIsCalculable(node->leftVal) || !ScalarExpIsCalculable(node->rightVal)) return false;
            KeyValue *leftKv = new_simple_key_value(NULL, ScalarExpGetValue(node->leftVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->leftVal)));
            KeyValue *rightKv = new_simple_key_value(NULL, ScalarExpGetValue(node->rightVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->rightVal)));
            return KeyValueEval((CompareType)node->opr, leftKv, rightKv);
        }
        /* We just think <like> or <in> include column(actually, it`s wrong, for example, 
         * the sql "select * from tb where 'a' in ('a', 'b', 'c')" is executable.), 
         * so just return fasle.*/
        case OP_LIKE:
        case OP_IN:
        case OP_NOT_LIKE:
        case OP_NOT_IN:
            return false;
        default:
            UNEXPECTED_VALUE(node->type);
            return false;
    }
}

/* For EXPR_AND_SET child, the final_true is true or not. */
static bool FinalTrueForExprAndSetChild(ExprNode *node) {
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        if (FinalTrueForVarChild(child)) continue;
        else return false;
    }
    return true;
}

static bool FinalTrueCaseExprOrSet(ExprNode *node) {
    Assert(node->type == EXPR_OR_SET);
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_OR_SET node, children just are EXPR_AND_SET or EXPR_VAR.  */
        switch (child->type) {
            case EXPR_AND_SET: {
                if (FinalTrueForExprAndSetChild(child)) return true;
                else break;
            }
            case EXPR_VAR: {
                if (FinalTrueForVarChild(child)) return true;
                else break;
            }
            default:
                UNEXPECTED_VALUE(node->type);
        }
    }
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

