#include <stdbool.h>
#include "data.h"
#include "select.h"

uint32_t IndexGetNextUnusedPageNum(MetaIndex *meta_index);
bool IndexDropByTableName(char *table_name);
int CompareKey(MetaIndex *meta_index, void *key1, void *key2);
bool IndexCreate(MetaIndex *meta_index);
MetaIndex *IndexLoad(Oid oid, Table *table);
bool IndexDrop(Oid oid);
bool IndexInsert(MetaIndex *meta_index, void *tuple, Refer *value);
void IndexSearchUnderCondition(SelectResult *result, SelectPlan *plan);

