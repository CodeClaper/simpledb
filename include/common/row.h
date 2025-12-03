#include "data.h"
#include "refer.h"

/* New a row. */
Row *NewRow();

/* Generate row by tuple. */
Row *GenerateRowInner(void *tuple, List *meta_columns);

/* Generate row by tuple. */
Row *GenerateRow(void *tuple, MetaTable *meta_table);

/* Fetch subrow. */
Row *FetchSubRow(Oid toid, Rid ref_id);

/* Seriable row to tuple. */
void *RowSeriableTuple(Row *row, Table *table);

/* Find the key in a row. */
void *RowFindKey(Row *row, Table *table);

/* Get row value or default. */
void *RowGetValueOrDefault(Row *row, MetaColumn *meta_column);
