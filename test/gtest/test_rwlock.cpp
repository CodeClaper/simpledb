#include "gtest/gtest.h"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

extern "C" {
#include "data.h"
#include "mmgr.h"
#include "rwlock.h"
}

static int *num;
#define COUNT_NUM 100000
#define WORKER_NUM 10

static void *work(RWLockEntry *lock) {
    int i;
    for (i = 0; i < COUNT_NUM; i++) {
        AcquireRWlock(lock, RW_WRITER);
        (*num)++;
        ReleaseRWlock(lock);
    }
    return NULL;
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
            work(lock);
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }
    ASSERT_EQ(1000000, *num);
}

