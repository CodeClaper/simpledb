#include "data.h"
#include "refer.h"

Rid InsertForTuple(Oid oid, void *key, void *tuple);
Rid InsertForRow(Table *table, Row *row);
void MakeupReservedColumns(Oid tid, Row *row);
InsertNode *GenerateInsertNode(char *table_name, List *value_list);
List *InsertForValues(InsertNode *insert_node);
List *ExecuteInsertStatement(InsertNode *insert_node);
