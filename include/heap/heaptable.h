#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 1

/* Create table inner. */
bool CreateHeapTableInner(Oid oid);

/* Create the heap table. */
bool CreateHeapTable(char *tableName);

/* Insert row data to heap table. */
Refer *HeapTableInsertRow(Table *table, Row *row);

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer);

