#include <stdbool.h>
#include "data.h"

bool CreateArrayHeapTable(Oid oid, Oid tid, char *table_name);
ArrayValue *QueryArrayValue(Refer *refer, MetaColumn *meta_column);
Refer *InsertArrayValue(Oid oid, ArrayValue *array, MetaColumn *meta_column);
bool DropArrayHeapTable(char *table_name);
