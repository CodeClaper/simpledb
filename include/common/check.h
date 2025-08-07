#include <stdbool.h>
#include "data.h"

/* Check value if valid. 
 * Because, CHAR, DATE, TIMESTAMP use '%s' format to pass value, thus check it. */
bool check_value_valid(MetaColumn *meta_column, AtomNode *atom_node);

/* Check SelectNode. */
bool check_select_node(SelectNode *select_node);

/* Check insert node. */
bool check_insert_node(InsertNode *insert_node);

/* Check for update node. */
bool check_update_node(UpdateNode *update_node);

/* Check for delete node. */
bool check_delete_node(DeleteNode *delete_node);

/* Check for create table node. */
bool check_create_table(CreateTableNode *create_table_node);

/* Chech allowed to drop table. */
bool check_drop_table(char *table_name);

/* Check for AlterTableNode. */
bool check_alter_table(AlterTableNode *alter_table);


