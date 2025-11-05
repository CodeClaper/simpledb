#include <stdbool.h>
#include "data.h"

/* Index create. */
bool IndexCreate(MetaIndex *meta_index);

/* Index load. */
MetaIndex *IndexLoad(Oid oid, Table *table);

/* Index drop. */
bool IndexDrop(char *index_name);

/* Index drop by table name. */
bool IndexDropByTableName(char *table_name);

/* Index insert. */
bool IndexInsert(MetaIndex *meta_index, void *tuple, Refer *value);
