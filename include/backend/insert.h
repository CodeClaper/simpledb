#include "data.h"
#include "refer.h"

/* Insert for tuple. */
Rid InsertForTuple(Oid oid, void *key, void *tuple);

/* Insert one row. */
Rid InsertForRow(Table *table, Row *row);

/* Makeup the system reserved column. */
void MakeupReservedColumns(Oid tid, Row *row);

/* Make a fake InsertNode. */
InsertNode *GenerateInsertNode(char *table_name, List *value_list);

/* Insert for values case. */
List *InsertForValues(InsertNode *insert_node);

/* Execute insert statement. */
List *ExecuteInsertStatement(InsertNode *insert_node);
