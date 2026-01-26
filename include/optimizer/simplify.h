#include "optimizer.h"

#define S_NONE (1<<0)               /* Set for all when don't know if success or fail. */
#define S_SUCCESS (1<<1)            /* Set for EXPR_VAR if success. */
#define S_FAIL (1<<2)               /* Set for EXPR_VAR if success. */
#define S_ONE_OF_SUCCESS (1<<3)     /* Set for EXPR_OR_SET if one of children success. */
#define S_ALL_FAIL (1<<4)           /* Set for EXPR_OR_SET if all children fail. */
#define S_ONE_OF_FAIL (1<<5)        /* Set for EXPR_AND_SET if one of children fail. */
#define S_ALL_SUCCESS (1<<6)        /* Set for EXPR_AND_SET if all children success. */


char* GetSimplifyResultName(int result);
ExprNode *Simplify(ExprNode *node);
