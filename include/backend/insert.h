#include "data.h"

/* Insert one row. */
Refer *insert_one_row(Table *table, Row *row);

/* Supplement system reserved column. */
void supple_reserved_column(Row *row);

/* Make a fake InsertNode. */
InsertNode *fake_insert_node(char *table_name, List *value_item_set_node);

/* Insert for values case. */
List *insert_for_values(InsertNode *insert_node);

/* Execute insert statement. */
List *exec_insert_statement(InsertNode *insert_node);
