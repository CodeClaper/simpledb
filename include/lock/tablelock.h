#include "exlock.h"

/* TableLockEntity. */
typedef struct TableLockEntity {
    Oid oid;
    ExLockEntry *entry_lock;
} TableLockEntity;

/* Initiliaze the table lock. */
void init_table_lock();

/* Check table if locked. */
void check_table_locked(Oid oid);

/* Try to acqurie the table. */
void try_acquire_table(Oid oid);

/* Try to release the table. */
void try_release_table(Oid oid);
    
