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


/* Initilise fdesc. */
void init_fdesc();

/* Unregister fdesc. */
void unregister_fdesc(Oid oid);

/* Get file descriptor. */
FDesc get_file_desc(Oid oid);
