#include "gtest/gtest.h"
#include <cstring>

extern "C" {
#include "queue.h"
#include "data.h"
#include "mmgr.h"
}

/* Test for create queue. */
TEST(queue, CreateQueue) {
    Queue *queue = CreateQueue(NODE_INT);
    ASSERT_NE(queue, NIQ);
    ASSERT_EQ(queue->size, 0);
}

/* Test for append queue. */
TEST(queue, AppendQueue) {
    Queue *queue = CreateQueue(NODE_INT);

    int i;
    for (i = 0; i < 30; i++) {
        AppendQueue(queue, &i);
    }
    ASSERT_EQ(queue->size, 30);

    QueueCell *qc;
    i = 0;
    qforeach(qc, queue) {
        ASSERT_EQ(qfirst_int(qc), i);
        i++;
    }
}

/* Test for delete queue. */
TEST(queue, DeleteQueue) {
    Queue *queue = CreateQueue(NODE_INT);

    int i;
    for (i = 0; i < 30; i++) {
        AppendQueue(queue, &i);
    }
    ASSERT_EQ(queue->size, 30);

    for (i = 0; i < 30; i = i + 2) {
        DeleteQueue(queue, &i);
    }

    ASSERT_EQ(queue->size,  15);
}
