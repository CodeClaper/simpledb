#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 0

/* Create table inner. */
bool CreateHeapTableInner(Oid oid);

/* Create the heap table. */
bool CreateHeapTable(char *tableName);

/* Direct insert heap table. */
Refer *HeapTableInsertRow(Oid oid, Row *row);

/* Loop up tuple from heap table. */
void *HeapTableLookupTuple(Table *table, Refer *refer);

/* Heap table iterator. */
void HeapTableIterator(Table *table, Refer *refer);

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer);

/* Update the row in heap table. */
void HeapTableUpdateRow(Table *table, Refer *refer, Row *row);

/* Update the heap table row createdXid. */
void HeapTableUpdateRowCreatedXid(Table *table, Refer *refer, Xid createdXid);

/* Update the heap table row createdXid. */
void HeapTableUpdateRowExpiredXid(Table *table, Refer *refer, Xid expiredXid);

/* Drop the heap table. */
bool DropHeapTable(char *tableName);

/* Heap table append new column. */
void HeapTableAppendColumn(Oid oid, MetaColumn *newColumn, int pos);

/* Heap table drop column. */
void HeapTableDropColumn(Oid oid, MetaColumn *oldColumn, int pos);
