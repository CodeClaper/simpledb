#include <stdbool.h>
#include <stdint.h>
#include "list.h"
#include "c.h"

#ifndef QUEUE_H
#define QUEUE_H

typedef struct Queue {
    NodeTag tag;
    volatile Size size;
    struct QueueCell *head;
    struct QueueCell *tail;
} Queue;

typedef struct QueueCell {
    ListCell *data;
    struct QueueCell *pres;
    struct QueueCell *next;
} QueueCell;

#define NIQ (Queue *)(NULL)

#define qfirst(qc) (((qc)->data)->ptr_value)
#define qfirst_int(qc) (((qc)->data)->int_value)
#define qfirst_bool(qc) (((qc)->data)->bool_value)
#define qfirst_float(qc) (((qc)->data)->float_value)
#define qfirst_double(qc) (((qc)->data)->double_value)
#define qfirst_long(qc) (((qc)->data)->long_value)

/* qforeach: a macro for looping throngh a queue. */
#define qforeach(qc, queue)\
    for (qc = queue->head; qc != NULL; qc = qc->next)

/* Function pointer about QueueConditon. */
typedef bool (*QUEUE_CONDTION)(QueueCell *qc);

/* Queue if empty. */
static inline bool QueueIsEmpty(Queue *queue) {
    return queue->size == 0;
}

/* Queue size. */
static inline int QueueSize(Queue *queue) {
    return queue->size;
}

/* Queue head. */
static inline QueueCell *QueueHead(Queue *queue) {
    return queue->head;
}

/* Queue tail. */
static inline QueueCell *QueueTail(Queue *queue) {
    return queue->tail;
}

Queue *CreateQueue(NodeTag tag);
void AppendQueue(Queue *queue, void *item);
void ConcatQueue(Queue *queue1, Queue *queue2);
void DeleteQueue(Queue *queue, void *item);
void DeleteQueueUnderCondition(Queue *queue, QUEUE_CONDTION condition);
void PushQueue(Queue *queue, void *item);
QueueCell *PopQueue(Queue *queue);
bool QueueMemberInt(Queue *queue, int item);
void FreeQueue(Queue *queue);

#endif
