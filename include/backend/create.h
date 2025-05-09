#include "data.h"

/* Combine user-level column. */
MetaColumn *combine_user_meta_column(ColumnDefNode *column_def, char *table_name);

/* Execute create table statement. */
void exec_create_table_statement(CreateTableNode *create_table_node, DBResult *result);
