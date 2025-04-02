#include <fcntl.h>
#include <stdint.h>
#include "spinlock.h"
#include "c.h"

#ifndef RWLOCK_H
#define RWLOCK_H

/* Rwlock mode. */
typedef enum RWLockMode {
    RW_INIT = 1,
    RW_READERS,
    RW_WRITER
} RWLockMode;

/* Rwlock Entry. */
typedef struct RWLockEntry {
    List *owner;                        /* Content lock owner rocesses. */
    volatile RWLockMode mode;           /* Rwlock mode. */
    volatile s_lock content_lock;       /* Content spinlock. */
    volatile s_lock sync_lock;          /* Sync spinlock. */
    volatile int waiting_reader;        /* Waiting readers number. */
    volatile int waiting_writer;        /* Waiting writers number. */
} RWLockEntry;

#define NOT_INIT_LOCK(entry) \
        (entry->mode != RW_INIT)

#define IS_READERS_LOCK(entry) \
        (entry->mode == RW_READERS)

#define IS_WRITER_LOCK(entry) \
        (entry->mode == RW_WRITER)

/* Init the rwlock. */
void InitRWlock(RWLockEntry *lock_entry);

/* Acuqire the rwlock. */
void AcquireRWlock(RWLockEntry *lock_entry, RWLockMode mode);

/* Release the rwlock. */
void ReleaseRWlock(RWLockEntry *lock_entry, RWLockMode mode);

#endif
