#include <stdbool.h>
#include "data.h"

/* Get index next unused page num. */
uint32_t IndexGetNextUnusedPageNum(MetaIndex *meta_index);

/* Index create. */
bool IndexCreate(MetaIndex *meta_index);

/* Index load. */
MetaIndex *IndexLoad(Oid oid, Table *table);

/* Index drop. */
bool IndexDrop(Oid oid);

/* Index drop by table name. */
bool IndexDropByTableName(char *table_name);

/* Index insert. */
bool IndexInsert(MetaIndex *meta_index, void *tuple, Refer *value);

/* Compare key. */
int CompareKey(MetaIndex *meta_index, void *key1, void *key2);
