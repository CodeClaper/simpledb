#include <stdbool.h>
#include "data.h"

/* Check value if valid. 
 * Because, CHAR, DATE, TIMESTAMP use '%s' format to pass value, thus check it. */
bool check_value_valid(MetaColumn *meta_column, AtomNode *atom_node);

/* Check SelectNode. */
bool CheckForSelect(SelectNode *select_node);

/* Check insert node. */
bool CheckForInsert(InsertNode *insert_node);

/* Check for update node. */
bool CheckForUpdate(UpdateNode *update_node);

/* Check for delete node. */
bool CheckForDelete(DeleteNode *delete_node);

/* Check for create table node. */
bool CheckForCreateTable(CreateTableNode *create_table_node);

/* Check for create index node. */
bool CheckForCreateIndex(CreateIndexNode *create_index_node);

/* Chech allowed to drop table. */
bool CheckForDropTable(char *table_name);

/* Check allowed to drop index. */
bool checkForDropIndex(char *index_name);

/* Check for AlterTableNode. */
bool CheckForAlterTable(AlterTableNode *alter_table);

/* Check for ShowNode. */
bool CheckForShow(ShowNode *show_node); 
