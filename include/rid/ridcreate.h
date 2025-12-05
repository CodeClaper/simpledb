#include <stdbool.h>
#include "data.h"

/* Rid create for system. */
bool CreateRidTableInner(Oid roid);

/* Rid create. */
bool CreateRidTable(Oid roid, Oid toid, char *table_name);

/* Drop the rid table. */
bool DropRidTable(Oid roid);

/* Shrink rid table. */
bool ShrinkRidTable(Oid roid);
