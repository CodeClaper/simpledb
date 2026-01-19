#ifndef __FLATTEN_H__
#define __FLATTEN_H__

#include <stdbool.h>
#include "data.h"
#include "optimizer.h"

ExprNode *ExprParse(SearchConditionNode *search_condition);
ExprNode *BNFTransform(ExprNode *node);
ExprNode *Negate(ExprNode *node);
ExprNode *Flatten(ExprNode *expr);
void ExprPrint(ExprNode *node);

#endif
