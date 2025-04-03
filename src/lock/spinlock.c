/******************************************* The Spin Lock Module *********************************************************
 * Auth:        JerryZhou
 * Created:     2024/01/24
 * Modify:      2024/01/24
 * Locataion:   src/memory/shmem.c
 * Description: The spin lock is the foundation of the locking mechanism
 * When waiting for a contented spin lock, we loop for a while and then try again. 
 * This routine is also known as Test-and-Set(TAS).
 ***************************************************************************************************************************
 * */
#include <stdint.h>
#include <unistd.h>
#include <stdatomic.h>
#include "spinlock.h"

/* Lock spin.
 * If cnt < 1024, it will PAUSE.
 * Else, it will sleep. 
 * */
int lock_spin(uint32_t cnt) {
    volatile int idx;
    /* Acutally, this should be cnt < 1024, then pause. 
     * But, pause might cause processor `starve`. 
     * We will intro phase-fair lock in the future, but now use sleep temporarily. */
	if (cnt < 1024 )
	  for (idx = 0; idx < cnt; idx++)
	    PAUSE();
	else
		return 1;
	return 0;
}

/* Lock sleep. */
void lock_sleep (int cnt) {
    struct timespec ts[1];
	ts->tv_sec = 0;
	ts->tv_nsec = cnt;
	nanosleep(ts, NULL);
}

/* Init spin lock. */
void init_spin_lock(volatile s_lock *lock) {
    *lock = SPIN_UN_LOCKED_STATUS;
}

/* Acquire spin lock, if fail, it will block. */
void acquire_spin_lock(volatile s_lock *lock) {
    while (__sync_lock_test_and_set(lock, 1)) {
        while (*lock) {
            if (lock_spin(DEFAULT_SPIN_INTERVAL))
                lock_sleep(DEFAULT_SPIN_INTERVAL);
        }
    }
}

/* Release spin lock. */
void release_spin_lock(volatile s_lock *lock) {
    Assert(*lock == SPIN_LOCKED_STATUS);
    /* Tell the c compiler and the CPU to not move loads or stores
     * past this point, to ensure that all the stores in the critical
     * section are visible to other CPUs before the  lock is released,
     * and that load in the critical seciton occrur strictly before
     * the lock is released. */
	__sync_synchronize();

    /* Release the lock, equivalent to lock_entry->content_lock = 0. 
     * This code does`t use a C assignment, since the C standard implies
     * that an assignment might be implemented with multiple store instructions. */
    __sync_lock_release(lock);

    /* Notice block processor. */
    NOTICE();
}

/* Wait for spin lock released. */
void wait_for_spin_lock(volatile s_lock *lock) {
    while (*lock) {
        if (lock_spin(DEFAULT_SPIN_INTERVAL))
            lock_sleep(DEFAULT_SPIN_INTERVAL);
    }
}
