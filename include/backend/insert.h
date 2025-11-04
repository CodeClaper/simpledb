#include "data.h"

/* Insert one row. */
Refer *InsertForRow(Table *table, Row *row);

/* Makeup the system reserved column. */
void MakeupReservedColumns(Row *row, char *table_name);

/* Make a fake InsertNode. */
InsertNode *GenerateInsertNode(char *table_name, List *value_list);

/* Insert for values case. */
List *InsertForValues(InsertNode *insert_node);

/* Execute insert statement. */
List *ExecuteInsertStatement(InsertNode *insert_node);
