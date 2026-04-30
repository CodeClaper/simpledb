#include "data.h"
#include <stdbool.h>

bool CreateArrHeapTable(Oid oid, Oid tid, char *table_name);
ArrayValue *QueryArrayValue(Refer *refer, int dim);
void InsertArrayValue(ArrayValue *array, int dim);
bool DropArrHeapTable(Oid oid);
