#include "data.h"

typedef int FDesc;

/* 
 * FDescEntry.
 * Store the relation of fdesc and table.
 */
typedef struct FDescEntry {
    Oid oid ;        /* Table oid */
    FDesc desc;      /* Table file descriptor. */
} FDescEntry;


void init_fdesc();
void unregister_fdesc(Oid oid);
FDesc get_file_desc(Oid oid);
