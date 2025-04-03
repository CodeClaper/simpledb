/******************************************* The Readers-Writer Lock Module *********************************************************
 * Auth:        JerryZhou
 * Created:     2024/12/11
 * Modify:      2024/12/11
 * Locataion:   src/lock/rwlock.c
 * Description: The readers-writer lock module supports the locking mechanism for reading and writing operations 
 * in concurrent scenarios.
 *
 * -------------------
 * The readers-writer lock has these features:
 * (1) The readers lock is a shared lock, it allowed other process to read the locked content and not allowd 
 * other process to write the locked content.
 * (2) The writer lock is an exclusive lock, it neither allowes other process to read nor other process to write 
 * the lock content.
 * (3) The reader lock is allowed to upgrade to write lock. This means reader lock can upgrade to write lock directly 
 * without release reader lock firstly. 
 * (4) If one process acquire the writer lock, it not allowed to acquire the rwlock lock again. This operation is
 * not useful but brings risks of concurrent security issues.
 ***************************************************************************************************************************
 * */

#include <stdbool.h>
#include <unistd.h>
#include <stdatomic.h>
#include "rwlock.h"
#include "mmgr.h"
#include "list.h"
#include "log.h"

static inline int GetCurrentPid() {
    return getpid();
}

/* Init the rwlock. */
void InitRWlock(RWLockEntry *lock_entry) {
    lock_entry->mode = RW_INIT;
    lock_entry->writer = 0;
    lock_entry->owner_num = 0;
    lock_entry->waiting_reader = 0;
    lock_entry->waiting_writer = 0;
    lock_entry->upgrading = false;
    init_spin_lock(&lock_entry->content_lock);
    init_spin_lock(&lock_entry->sync_lock);
}


/* The condition to keep fair. 
 * The rwlock provides simple fairness mechanism to avoid starve.
 * (1) Must writer (if these is any writer) acquire the lock after reader release the lock.
 * (2) Must reader (if these is any reader) acquire the lock after writer release the lock.
 * */
static inline bool FairCondition(RWLockEntry *lock_entry, Pid curpid, RWLockMode mode) {
    switch (lock_entry->mode) {
        case RW_READERS:
            if (lock_entry->waiting_writer > 0 && mode == RW_READERS)
                return false;
            break;
        case RW_WRITER:
            if (lock_entry->waiting_reader > 0 && mode == RW_WRITER)
                return false;
            break;
        default:
            ;
    }
    return true;
}

/* The reenter condition.
 * Reenter condition includes:
 * (1) The current process is the only one who has owned the rwlock.
 * (2) RWLock in RW_READERS mode, and the request mode also is RW_READERS, and the lock is not upgrading. 
 * */
static inline bool ReenterCondition(RWLockEntry *lock_entry, Pid curpid, RWLockMode mode) {
    return (lock_entry->mode == RW_WRITER && lock_entry->writer == curpid) || 
            (lock_entry->mode == RW_READERS && mode == RW_READERS && lock_entry->owner_num > 0 && !lock_entry->upgrading);
}

/* Increase Waiting. */
static inline void IncreaseWaiting(RWLockEntry *lock_entry, RWLockMode mode) {
    switch (mode) {
        case RW_READERS:
            __sync_fetch_and_add(&lock_entry->waiting_reader, 1);
            break;
        case RW_WRITER:
            __sync_fetch_and_add(&lock_entry->waiting_writer, 1);
            break;
        default:
            ;
    }
}

/* Decrease Waiting. */
static inline void DecreaseWaiting(RWLockEntry *lock_entry, RWLockMode mode) {
    switch (mode) {
        case RW_READERS:
            if (lock_entry->waiting_reader > 0)
                __sync_fetch_and_sub(&lock_entry->waiting_reader, 1);
            break;
        case RW_WRITER:
            if (lock_entry->waiting_writer > 0)
                __sync_fetch_and_sub(&lock_entry->waiting_writer, 1);
            break;
        default:
            ;
    }
}

/* Increase owner. */
static inline void IncreaseOwner(RWLockEntry *lock_entry) {
    __sync_fetch_and_add(&lock_entry->owner_num, 1);
}

/* Increase owner. */
static inline void DecreaseOwner(RWLockEntry *lock_entry) {
    Assert(lock_entry->owner_num > 0);
    __sync_fetch_and_sub(&lock_entry->owner_num, 1);
}

 
/* Try acquire the rwlock. 
 * Two ways to acquire the rwlock:
 * (1) Directly acquire the rwlock and satisfy the fair condition.
 * (2) Not acquired directly the rwlock but satisfy the reenter condition and the fair condition.
 * */
static void AcquireRWLockInner(RWLockEntry *lock_entry, RWLockMode mode) {
    Pid cur_pid = GetCurrentPid();
    /* Add waiting count. */
    IncreaseWaiting(lock_entry, mode);
retry_lab:
    while (__sync_lock_test_and_set(&lock_entry->content_lock, 1)) {
        while (lock_entry->content_lock) {
            if (ReenterCondition(lock_entry, cur_pid, mode)) 
                goto acquire_lock_lab;

            if (lock_spin(DEFAULT_SPIN_INTERVAL))
                lock_sleep(DEFAULT_SPIN_INTERVAL);
        }
    }
acquire_lock_lab:
    acquire_spin_lock(&lock_entry->sync_lock);
    /* Avoid writer acquire the lock after a reder reenter. */
    if (mode == RW_WRITER && lock_entry->mode == RW_READERS && lock_entry->owner_num > 0) {
        release_spin_lock(&lock_entry->sync_lock);
        goto retry_lab;
    }
    /* Avoid reader reenter adter a writer acquire the lock.  
     * Avoid reader reenter after before reader has upgraded. */
    if (mode == RW_READERS && lock_entry->mode == RW_WRITER && lock_entry->writer != cur_pid) {
        release_spin_lock(&lock_entry->sync_lock);
        goto retry_lab;
    }
    lock_entry->content_lock = SPIN_LOCKED_STATUS;
    DecreaseWaiting(lock_entry, mode);
    if (mode == RW_WRITER)
        lock_entry->writer = cur_pid;
    IncreaseOwner(lock_entry);
    if (mode > lock_entry->mode)
        lock_entry->mode = mode;
    release_spin_lock(&lock_entry->sync_lock);
}

/* Try to release rwlock.*/
static inline void ReleaseRWLockInner(RWLockEntry *lock_entry) {
    /* Tell the c compiler and the CPU to not move loads or stores
     * past this point, to ensure that all the stores in the critical
     * section are visible to other CPUs before the  lock is released,
     * and that load in the critical seciton occrur strictly before
     * the lock is released. */
	__sync_synchronize();

    /* Release the lock, equivalent to lock_entry->content_lock = 0. 
     * This code does`t use a C assignment, since the C standard implies
     * that an assignment might be implemented with multiple store instructions. */
    __sync_lock_release(&lock_entry->content_lock);

    lock_entry->mode = RW_INIT;
    lock_entry->writer = 0;

    /* Notice block processor. */
    NOTICE();
}

/* Acuqire the rwlock. */
void AcquireRWlock(RWLockEntry *lock_entry, RWLockMode mode) {
    Assert(mode != RW_INIT);
    /* Not alloed same process acquire the writer lock again. */
    AssertFalse(lock_entry->mode == RW_WRITER && 
                    mode == RW_WRITER && 
                         lock_entry->writer == GetCurrentPid());
    AcquireRWLockInner(lock_entry, mode); 
}

/* Upgrade the rwlock. 
 * -------------------
 * Upgrade means the lock mode changes from RW_READERS to RW_WRITER. */
void UpgradeRWlock(RWLockEntry *lock_entry) {
    Assert(NOT_INIT_LOCK(lock_entry));
    Assert(lock_entry->mode == RW_READERS);
    Assert(lock_entry->content_lock == SPIN_LOCKED_STATUS);
    
    DecreaseOwner(lock_entry);

    /* Lock sync_lock. */
    acquire_spin_lock(&lock_entry->sync_lock);

    lock_entry->upgrading = true;
    
    /* Wating for all reader lock release. */
    while (lock_entry->owner_num > 0) {
        lock_sleep(DEFAULT_SPIN_INTERVAL);
    }
    
    /* Acuqire the write lock. */
    lock_entry->mode = RW_WRITER;
    lock_entry->owner_num = 1;
    lock_entry->writer = GetCurrentPid();
    lock_entry->upgrading = false;

    /* Relase sync_lock. */
    release_spin_lock(&lock_entry->sync_lock);
}


/* Downgrade the rwlock. 
 * -------------------
 * Downgrade means the lock mode changes from RW_WRITER to RW_READERS. */
void DowngradeRWlock(RWLockEntry *lock_entry) {
    Assert(NOT_INIT_LOCK(lock_entry));
    Assert(lock_entry->mode == RW_WRITER);
    Assert(lock_entry->owner_num == 1);
    Assert(lock_entry->writer == GetCurrentPid());
    Assert(lock_entry->content_lock == SPIN_LOCKED_STATUS);

    lock_entry->mode = RW_READERS;
}

/* Release the rwlock. */
void ReleaseRWlock(RWLockEntry *lock_entry) {
    /* There is occasional bug here. */
    Assert(NOT_INIT_LOCK(lock_entry));
    Assert(LOCKED(lock_entry->content_lock));

    /* It`s import this DecreaseOwner caller out of the scope of sync_lock control. 
     * DecreaseOwner has memory barrier, so sync lock is not necessary. 
     * More important, when upgrade lock, we will wait for the owner_num decrease to zero. */
    DecreaseOwner(lock_entry);

    acquire_spin_lock(&lock_entry->sync_lock);
    if (lock_entry->owner_num == 0)
        ReleaseRWLockInner(lock_entry);
    release_spin_lock(&lock_entry->sync_lock);
}
