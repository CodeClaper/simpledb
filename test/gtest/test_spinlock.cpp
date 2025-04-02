#include "gtest/gtest.h"
#include <cstddef>
#include <pthread.h>

extern "C" {
#include "data.h"
#include "mmgr.h"
#include "spinlock.h"
}

static int num = 0;
static s_lock lock = SPIN_UN_LOCKED_STATUS;
#define COUNT_NUM 100000
#define WORKER_NUM 100

static void *work(void *arg) {
    int i;
    for (i = 0; i < COUNT_NUM; i++) {
        acquire_spin_lock(&lock);
        num++;
        release_spin_lock(&lock);
    }
    return NULL;
}

TEST(spinlock, test_concurrent) {
    pthread_t workers[WORKER_NUM];
    int i;
    for (i = 0; i < WORKER_NUM; i++) {
        pthread_create(&workers[i], NULL, work, NULL);
    }
    for (i = 0; i < WORKER_NUM; i++) {
        pthread_join(workers[i], NULL);
    }
    ASSERT_EQ(10000000, num);
}
