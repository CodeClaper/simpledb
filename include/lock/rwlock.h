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
    Pid writer;                         /* Process owned the write lock. */
    volatile int owner_num;             /* Owner num. */
    volatile RWLockMode mode;           /* Rwlock mode. */
    volatile s_lock content_lock;       /* Content spinlock. */
    volatile s_lock sync_lock;          /* Sync spinlock. */
    volatile int waiting_reader;        /* Waiting readers number. */
    volatile int waiting_writer;        /* Waiting writers number. */
    bool upgrading;                     /* The flag if upgrading. */
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

/* Upgrade the rwlock. 
 * -------------------
 * Upgrade means the lock mode changes from RW_READERS to RW_WRITER. */
void UpgradeRWlock(RWLockEntry *lock_entry);

/* Downgrade the rwlock. 
 * -------------------
 * Downgrade means the lock mode changes from RW_WRITER to RW_READERS. */
void DowngradeRWlock(RWLockEntry *lock_entry);

/* Release the rwlock. */
void ReleaseRWlock(RWLockEntry *lock_entry, RWLockMode mode);

#endif
