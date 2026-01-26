#include "optimizer.h"

#define S_NONE (1<<0)               /* For all. */
#define S_SUCCESS (1<<1)            /* For EXPR_VAR. */
#define S_FAIL (1<<2)               /* For EXPR_VAR. */
#define S_ONE_OF_SUCCESS (1<<3)     /* For EXPR_OR_SET. */
#define S_ALL_FAIL (1<<4)           /* For EXPR_OR_SET. */
#define S_ONE_OF_FAIL (1<<5)        /* For EXPR_AND_SET. */
#define S_ALL_SUCCESS (1<<6)        /* For EXPR_AND_SET. */

ExprNode *Simplify(ExprNode *node);
char* GetSimplifyResultName(int result);
