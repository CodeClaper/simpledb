#include "data.h"

/* Get value in tuple. */
void *TupleFindValue(void *tuple, MetaColumn *meta_column);

/* Get primary key value in tuple. */
void *TupleFindKey(void *tuple, MetaTable *meta_table);

/* Get created xid in tuple. */
Xid TupleFindCreatedXid(void *tuple, MetaTable *meta_table);

/* Get created xid in tuple. */
Xid TupleFindExpiredXid(void *tuple, MetaTable *meta_table);
