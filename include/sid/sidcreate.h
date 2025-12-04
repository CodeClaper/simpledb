#include <stdbool.h>
#include "data.h"

/* Sid create for system. */
bool CreateSidTableInner(Oid roid);

/* Sid create. */
bool CreateSidTable(Oid roid, Oid toid, char *table_name);

/* Shrink sid table. */
bool ShrinkSidTable(Oid roid);
