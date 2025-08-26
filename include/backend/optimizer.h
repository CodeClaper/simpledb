#include "select.h"

/* Optimize Select Statment. */
SelectPlan *OptimizeSelect(SelectNode *selectNode, StatementType stmt_type);

/* Generate a Simple SelectPlan. */
SelectPlan *SimpleSelectPlan(ROW_HANDLER rowHanler, ROW_HANDLER_ARG_TYPE type, void *arg);

