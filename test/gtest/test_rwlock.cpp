#include "gtest/gtest.h"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

extern "C" {
#include "data.h"
#include "mmgr.h"
#include "rwlock.h"
#include "log.h"
}

static int *num;
#define COUNT_NUM 100000
#define WORKER_NUM 20

static void work_with_writelock(RWLockEntry *lock) {
    int i;
    for (i = 0; i < COUNT_NUM; i++) {
        AcquireRWlock(lock, RW_WRITER);
        (*num)++;
        ReleaseRWlock(lock);
    }
}

static void work_with_upgradlock(RWLockEntry *lock) {
    int i, current;
    for (i = 0; i < COUNT_NUM; i++) {
        AcquireRWlock(lock, RW_READERS);
        // read num
        current = *num;
        UpgradeRWlock(lock);
        // write num
        (*num)++;
        ReleaseRWlock(lock);
    }
}

static void work_with_upgradlock_then_downgrade(RWLockEntry *lock) {
    int i, current;
    for (i = 0; i < COUNT_NUM; i++) {
        AcquireRWlock(lock, RW_READERS);
        // read num
        current = *num;
        UpgradeRWlock(lock);
        // write num
        (*num)++;
        DowngradeRWlock(lock);
        // read num again
        current = *num;
        ReleaseRWlock(lock);
    }
}

static void reader_work(RWLockEntry *lock) {
    AcquireRWlock(lock, RW_READERS);
    int current = *num;
    ASSERT_EQ(0, current % 2);
    sleep(2);
    ReleaseRWlock(lock);
}

static void writer_work(RWLockEntry *lock) {
    AcquireRWlock(lock, RW_READERS);
    (*num)++;
    (*num)++;
    sleep(2);
    ReleaseRWlock(lock);
}

TEST(rwlock, test_rwlock_concurrent) {
    switch_shared();
    num = instance(int);
    *num = 0;
    RWLockEntry *lock = instance(RWLockEntry);
    InitRWlock(lock);
    switch_local();

    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        Pid pid  = fork();
        if (pid < 0) {
            perror("Bad fork");
            exit(1);
        } else if (pid == 0) {
            work_with_writelock(lock);
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }

    ASSERT_EQ(2000000, *num);
    ASSERT_EQ(0, lock->waiting_writer);
    ASSERT_EQ(0, lock->waiting_reader);
}

TEST(rwlock, test_rwlock_upgrade) {
    switch_shared();
    num = instance(int);
    *num = 0;
    RWLockEntry *lock = instance(RWLockEntry);
    InitRWlock(lock);
    switch_local();

    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        Pid pid  = fork();
        if (pid < 0) {
            perror("Bad fork");
            exit(1);
        } else if (pid == 0) {
            work_with_upgradlock(lock);
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }

    ASSERT_EQ(2000000, *num);
    ASSERT_EQ(0, lock->waiting_writer);
    ASSERT_EQ(0, lock->waiting_reader);
}

TEST(rwlock, test_rwlock_upgrade_and_downgrade) {
    switch_shared();
    num = instance(int);
    *num = 0;
    RWLockEntry *lock = instance(RWLockEntry);
    InitRWlock(lock);
    switch_local();

    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        Pid pid  = fork();
        if (pid < 0) {
            perror("Bad fork");
            exit(1);
        } else if (pid == 0) {
            work_with_upgradlock_then_downgrade(lock);
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }

    ASSERT_EQ(2000000, *num);
    ASSERT_EQ(0, lock->waiting_writer);
    ASSERT_EQ(0, lock->waiting_reader);
}

TEST(rwlock, test_current) {
    switch_shared();
    num = instance(int);
    *num = 0;
    RWLockEntry *lock = instance(RWLockEntry);
    InitRWlock(lock);
    switch_local();

    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        Pid pid  = fork();
        if (pid < 0) {
            perror("Bad fork");
            exit(1);
        } else if (pid == 0) {
            if (i % 2)
                writer_work(lock);
            else
                reader_work(lock);
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }

    ASSERT_EQ(20, *num);
    ASSERT_EQ(0, lock->waiting_writer);
    ASSERT_EQ(0, lock->waiting_reader);
}
