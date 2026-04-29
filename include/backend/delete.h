#include "data.h"
#include "select.h"

void delete_row(void *destin, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);
void exec_delete_statement(DeleteNode *delete_node, DBResult *result);
