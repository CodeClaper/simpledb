#include "data.h"

/* Combine user-level column. */
MetaColumn *ColumnDefNodeGenerateMetaColumn(ColumnDefNode *column_def, char *table_name);

/* Execute create table statement. */
void ExecuteCreateTableStatement(CreateTableNode *create_table_node, DBResult *result);

/* Execute create index statement. */
void ExecuteCreateIndexStatement(CreateIndexNode *create_index_node, DBResult *result);
