#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 0

/* Create table inner. */
bool CreateHeapTableInner(Oid hoid);

/* Create the heap table. */
bool CreateHeapTable(Oid toid, char *tableName);

/* Insert heap table. */
Refer *HeapTableInsertTuple(Oid oid, void *tuple);

/* Loop up tuple from heap table. */
void *HeapTableLookupTuple(Oid oid, Refer *refer);

/* Heap table iterator. */
void HeapTableIteratorRefer(Refer *refer);

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Oid oid, Refer *refer);

/* Update the row in heap table. */
void HeapTableUpdateTuple(Oid oid, Refer *refer, void *tuple);

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
