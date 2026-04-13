#include "data.h"
#include "refer.h"

Row *NewRow();
Row *GenerateRowInner(void *tuple, List *meta_columns);
Row *GenerateRow(void *tuple, MetaTable *meta_table);
Row *FetchSubRow(Oid toid, Rid ref_id);
void *RowSeriableTuple(Row *row, Table *table);
void *RowFindKey(Row *row, Table *table);
void *RowGetValueOrDefault(Row *row, MetaColumn *meta_column);
