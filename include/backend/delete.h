#include "data.h"
#include "select.h"

/* Delete row */
void delete_row(void *destin, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg);

/* Execute delete statement. */
void exec_delete_statement(DeleteNode *delete_node, DBResult *result);
