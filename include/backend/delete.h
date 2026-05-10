#include "data.h"
#include "optimizer.h"

void delete_row(void *tuple, SelectResult *select_result, SelectPlan *select_plan);
void exec_delete_statement(DeleteNode *delete_node, DBResult *result);
