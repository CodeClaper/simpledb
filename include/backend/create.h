#include "data.h"

/* Combine user-level column. */
MetaColumn *ColumnDefNodeGenerateMetaColumn(ColumnDefNode *column_def, char *table_name);

/* Execute create table statement. */
void exec_create_table_statement(CreateTableNode *create_table_node, DBResult *result);
