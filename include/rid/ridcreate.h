#include "data.h"
#include <stdbool.h>

/* Rid create for system. */
bool CreateRidTableInner(Oid roid);

/* Rid create. */
bool CreateRidTable(Oid roid, Oid toid, char *table_name);
