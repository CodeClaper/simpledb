#include "data.h"

/* New a row. */
Row *NewRow();

/* Generate row by tuple. */
Row *GenerateRowInner(void *tuple, List *meta_columns);

/* Generate row by tuple. */
Row *GenerateRow(void *tuple, MetaTable *meta_table);

/* Find the key in a row. */
void *RowFindKey(Row *row, MetaTable *meta_table);

/* Get row value or default. */
void *RowGetValueOrDefault(Row *row, MetaColumn *meta_column);
