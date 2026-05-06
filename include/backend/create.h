#include "data.h"

MetaColumn *ColumnDefNodeGenerateMetaColumn(Oid tid, Oid stid, Oid aoid, ColumnDefNode *column_def);
void ExecuteCreateTableStatement(CreateTableNode *create_table_node, DBResult *result);
void ExecuteCreateIndexStatement(CreateIndexNode *create_index_node, DBResult *result);
