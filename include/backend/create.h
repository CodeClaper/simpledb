#include "data.h"

/* Combine user-level column. */
MetaColumn *ColumnDefNodeGenerateMetaColumn(Oid tid, Oid stid, ColumnDefNode *column_def);

/* Execute create table statement. */
void ExecuteCreateTableStatement(CreateTableNode *create_table_node, DBResult *result);

/* Execute create index statement. */
void ExecuteCreateIndexStatement(CreateIndexNode *create_index_node, DBResult *result);
