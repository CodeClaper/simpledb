#include "gtest/gtest.h"
#include <cstddef>
#include <pthread.h>

extern "C" {
#include "data.h"
#include "mmgr.h"
#include "spinlock.h"
}

static int *num;
static s_lock *lock;
#define COUNT_NUM 100000
#define WORKER_NUM 20

static void *work() {
    int i;
    for (i = 0; i < COUNT_NUM; i++) {
        acquire_spin_lock(lock);
        (*num)++;
        release_spin_lock(lock);
    }
    return NULL;
}

TEST(spinlock, test_concurrent) {
    switch_shared();
    num = instance(int);
    *num = 0;
    lock = instance(s_lock);
    init_spin_lock(lock);
    switch_local();

    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        Pid pid  = fork();
        if (pid < 0) {
            perror("Bad fork");
            exit(1);
        } else if (pid == 0) {
            work();
            exit(0);
        }
    }

    for (i = 0; i < WORKER_NUM; i++) {
        wait(NULL);
    }
    ASSERT_EQ(2000000, *num);
}
