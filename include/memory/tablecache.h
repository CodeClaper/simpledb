#include <stdbool.h>
#include "data.h"

void InitTableCache();
List *GetAllTableCache();
void SaveTableCache(Table *table);
bool TableExistsInCache(Oid oid);
bool TableNameExistsInCache(char *tableName);
bool RemoveTableCache(Oid oid);
Table *FindTableCache(Oid oid);
Table *NameFindTableCache(char *tableName);
