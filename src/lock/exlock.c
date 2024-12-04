/****************************** The Exclusive Lock ***************************
 * Auth:        JerryZhou
 * Created:     2024/08/10
 * Modify:      2024/08/10
 * Locataion:   src/lock/exlock.c
 * Description: The exclusive lock is a lock which only allowed the same processor 
 * acuqires the lock. Ohters processors will wail until releasing the lock.
******************************************************************************/

#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <unistd.h>
#include "spinlock.h"
#include "exlock.h"

/* Init exclusive lock. */
void init_exlock(ExLockEntry *lock_entry) {
    Assert(lock_entry);
    lock_entry->pid = 0;
    init_spin_lock(&lock_entry->lock);
}

/* Acqure the exclusive lock. */
void acquire_exlock(ExLockEntry *lock_entry) {
    Assert(lock_entry);
    pid_t pid = getpid();
    /* If current processor is the one that aleary 
     * acuqiring the lock, return.*/
    if (pid == lock_entry->pid)
        return;
    acquire_spin_lock(&lock_entry->lock);
    lock_entry->pid = pid;
}

/* Release the exclusive lock. */
void release_exlock(ExLockEntry *lock_entry) {
    Assert(lock_entry);
    pid_t pid = getpid();
    /* Only the same processor that has acuqired the lock can release the lock.*/
    Assert(pid == lock_entry->pid);

    release_spin_lock(&lock_entry->lock);
    lock_entry->pid = 0;
}

/* Wait for exlock released. */
void wait_for_exlock(ExLockEntry *lock_entry) {
    Assert(lock_entry);
    pid_t pid = getpid();
    /* If current processor is the one that aleary acuqiring the lock, return.*/
    if (pid == lock_entry->pid)
        return;
    wait_for_spin_lock(&lock_entry->lock);
}

