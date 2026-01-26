#include <stdbool.h>
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

/* Simplify When case VAR. */
static int SimplifyCaseVar(ExprNode *node) {
    switch (node->opr) {
        case OP_EQ: 
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE: {
            if (!ScalarExpIsCalculable(node->leftVal) || !ScalarExpIsCalculable(node->rightVal)) return S_NONE;
            KeyValue *leftKv = new_simple_key_value(NULL, ScalarExpGetValue(node->leftVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->leftVal)));
            KeyValue *rightKv = new_simple_key_value(NULL, ScalarExpGetValue(node->rightVal), AtomTypeConvertDataType(ScalarExpGetAtomType(node->rightVal)));
            return KeyValueEval((CompareType)node->opr, leftKv, rightKv) ? S_SUCCESS : S_FAIL;
        }
        /* We just think <like> or <in> include column(actually, it`s wrong, for example, 
         * the sql "select * from tb where 'a' in ('a', 'b', 'c')" is executable.), 
         * so just return fasle.*/
        case OP_LIKE:
        case OP_IN:
        case OP_NOT_LIKE:
        case OP_NOT_IN:
            return S_NONE;
        default:
            UNEXPECTED_VALUE(node->type);
            return S_NONE;
    }
}

/* Simplify when case EXPR_AND_SET. */
static int SimplifyCaseExprAndSet(ExprNode *node) {
    Assert(node->type == EXPR_AND_SET);
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_AND_SET node, children just are EXPR_VAR.  */
        switch (child->type) {
            case EXPR_VAR: {
                int ret = SimplifyCaseVar(child); 
                if (ret == S_FAIL) return S_ONE_OF_FAIL;
                else if (ret == S_NONE) return S_NONE;
                else break;
            }
            default:
                UNEXPECTED_VALUE(node->type);
        }
    }
    return S_ALL_SUCCESS;
}

/* Simplify when case EXPR_OR_SET. */
static int SimplifyCaseExprOrSet(ExprNode *node) {
    Assert(node->type == EXPR_OR_SET);
    ListCell *lc;
    foreach(lc, node->children) {
        ExprNode *child = (ExprNode *) lfirst(lc);
        /* For EXPR_OR_SET node, children just are EXPR_AND_SET or EXPR_VAR.  */
        switch (child->type) {
            case EXPR_AND_SET: {
                child->sresult = SimplifyCaseExprAndSet(child);
                if (child->sresult == S_ALL_SUCCESS) return S_ONE_OF_SUCCESS;
                else break;
            }
            case EXPR_VAR: {
                int ret = SimplifyCaseVar(child);
                if (ret == S_SUCCESS) return S_ONE_OF_SUCCESS ;
                else break;
            }
            default:
                UNEXPECTED_VALUE(node->type);
        }
    }
    return S_ALL_FAIL;
}

/* Simplify. */
ExprNode *Simplify(ExprNode *node) {
    switch (node->type) {
        case EXPR_VAR:
            node->sresult = SimplifyCaseVar(node);
            break;
        case EXPR_AND_SET:
            node->sresult = SimplifyCaseExprAndSet(node); 
            break;
        case EXPR_OR_SET: 
            node->sresult = SimplifyCaseExprOrSet(node); 
            break;
        default: break;
    }
    return node;
}

/* Get simplify reult name. */
char* GetSimplifyResultName(int result) {
    switch (result) {
        case S_NONE: return "none";
        case S_SUCCESS: return "success";
        case S_FAIL: return "fail";
        case S_ONE_OF_SUCCESS: return "one of success";
        case S_ALL_FAIL: return "all fail";
        case S_ONE_OF_FAIL: return "one of fail";
        case S_ALL_SUCCESS: return "all success";
        default: return "none";
    }
}


