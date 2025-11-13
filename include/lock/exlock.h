#include "spinlock.h"
#include <stdint.h>

#ifndef EXLOCK_H
#define EXLOCK_H

typedef struct ExLockEntry {
    volatile s_lock lock;           /* The lock. */
    volatile pid_t pid;             /* The pid that acquiring the lock.*/
    volatile uint32_t refcount;     /* Referenct count. */
} ExLockEntry;

/* Init exclusive lock. */
void init_exlock(ExLockEntry *lock_entry);

/* Acqure the exclusive lock. */
void acquire_exlock(ExLockEntry *lock_entry);

/* Release the exclusive lock. */
void release_exlock(ExLockEntry *lock_entry);

/* Wait for exlock released. */
void wait_for_exlock(ExLockEntry *lock_entry);

#endif
