#include "data.h"
#include "select.h"

/* Delete row */
void delete_row(Row *row, SelectResult *select_result, Table *table, 
                ROW_HANDLER_ARG_TYPE type, void *arg);

/* Execute delete statement. */
void exec_delete_statement(DeleteNode *delete_node, DBResult *result);
