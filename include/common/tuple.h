#include "data.h"

/* Get value in tuple. */
void *TupleFindValue(void *tuple, MetaColumn *meta_column);

/* Get primary key value in tuple. */
void *TupleFindKey(void *tuple, MetaTable *meta_table);
