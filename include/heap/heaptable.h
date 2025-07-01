#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 0

/* Create table inner. */
bool CreateHeapTableInner(Oid oid);

/* Create the heap table. */
bool CreateHeapTable(char *tableName);

/* Insert row data to heap table. */
Refer *HeapTableInsertRow(Cursor *cursor, Row *row);

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer);

/* Update the row in heap table. */
void HeapTableUpdateRow(Table *table, Refer *refer, Row *row);

/* Update the index refer in row. */
void HeapTableUpdateIndexRefer(Table *table, Refer *refer, Refer *newIRefer);

/* Update the heap table row createdXid. */
void HeapTableUpdateRowCreatedXid(Table *table, Refer *refer, Xid createdXid);

/* Update the heap table row createdXid. */
void HeapTableUpdateRowExpiredXid(Table *table, Refer *refer, Xid expiredXid);

/* Drop the heap table. */
bool DropHeapTable(char *tableName);

/* Heap table append new column. */
void HeapTableAppendColumn(Table *table, MetaColumn *newColumn, int pos);

/* Heap table drop column. */
void HeapTableDropColumn(Table *table, MetaColumn *oldColumn, int pos);
