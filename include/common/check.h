#include <stdbool.h>
#include "data.h"

bool check_value_valid(MetaColumn *meta_column, AtomNode *atom_node);
bool CheckForSelect(SelectNode *select_node);
bool CheckForInsert(InsertNode *insert_node);
bool CheckForUpdate(UpdateNode *update_node);
bool CheckForDelete(DeleteNode *delete_node);
bool CheckForCreateTable(CreateTableNode *create_table_node);
bool CheckForCreateIndex(CreateIndexNode *create_index_node);
bool CheckForDropTable(char *table_name);
bool checkForDropIndex(char *index_name);
bool CheckForAlterTable(AlterTableNode *alter_table);
bool CheckForShow(ShowNode *show_node); 
bool CheckForExplain(ExplainNode *explain_node);
bool CheckForExpress(ExpressNode *express_node);
