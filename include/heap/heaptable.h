#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 1

/* Create the heap table. */
bool CreateHeapTable(char *tableName);

/* Insert row data to heap table. */
Refer *HeapTableInsertRow(Oid oid, Row *row);

/* Query row from heap table. */
Row *HeapTableQueryRow(Refer *refer);

