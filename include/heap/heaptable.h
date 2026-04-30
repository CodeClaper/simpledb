#include <stdbool.h>
#include "refer.h"

#define HEAP_TABLE_ROOT_PAGE 0
#define HEAP_TABLE_FIRST_CELL_NUM 0

bool CreateHeapTableInner(Oid hoid);
bool CreateHeapTable(Oid oid, Oid toid, char *tableName);
Refer *HeapTableInsertTuple(Oid oid, void *tuple);
void *HeapTableLookupTuple(Oid oid, Refer *refer);
void HeapTableIteratorRefer(Refer *refer);
Row *HeapTableLookupRow(Oid oid, Refer *refer);
void HeapTableUpdateTuple(Oid oid, Refer *refer, void *tuple);
void HeapTableUpdateRowCreatedXid(Table *table, Refer *refer, Xid createdXid);
void HeapTableUpdateRowExpiredXid(Table *table, Refer *refer, Xid expiredXid);
bool DropHeapTable(Oid hoid);
void HeapTableAppendColumn(Oid oid, MetaColumn *newColumn, int pos);
void HeapTableDropColumn(Oid oid, MetaColumn *oldColumn, int pos);
