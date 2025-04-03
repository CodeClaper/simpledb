#include <stdbool.h>
#include <string.h>
#include "queue.h"
#include "mmgr.h"

/* Create a Queue. */
Queue *CreateQueue(NodeTag tag) {
    Queue *queue = instance(Queue);
    queue->tag = tag;
    queue->size = 0;
    queue->head = NULL;
    queue->tail = NULL;
    return queue;
}

/* Append QueueCell to the Queue. */
static void AppendQueueCell(Queue *queue, QueueCell *cell) {
    if (queue->head == NULL && queue->tail == NULL) {
        queue->head = cell;
        queue->tail = cell;
    } else {
        QueueCell *tail = queue->tail;
        tail->next = cell;
        cell->pres = tail;
        queue->tail = cell;
    }
    queue->size++;
}

/* Append int item to the Queue. */
static void AppendQueueInt(Queue *queue, int item) {
    QueueCell *cell = instance(QueueCell);
    ListCell *lc = instance(ListCell);
    lc->int_value = item;
    cell->data = lc;
    cell->next = NULL;
    cell->pres = NULL;
    AppendQueueCell(queue, cell);
}

/* Append bool item to the Queue. */
static void AppendQueueBool(Queue *queue, bool item) {
    QueueCell *cell = instance(QueueCell);
    ListCell *lc = instance(ListCell);
    lc->bool_value = item;
    cell->data = lc;
    cell->next = NULL;
    cell->pres = NULL;
    AppendQueueCell(queue, cell);
}

/* Append float item to the Queue. */
static void AppendQueueFloat(Queue *queue, float item) {
    QueueCell *cell = instance(QueueCell);
    ListCell *lc = instance(ListCell);
    lc->float_value = item;
    cell->data = lc;
    cell->next = NULL;
    cell->pres = NULL;
    AppendQueueCell(queue, cell);
}

/* Append double item to the Queue. */
static void AppendQueueDouble(Queue *queue, double item) {
    QueueCell *cell = instance(QueueCell);
    ListCell *lc = instance(ListCell);
    lc->double_value = item;
    cell->data = lc;
    cell->next = NULL;
    cell->pres = NULL;
    AppendQueueCell(queue, cell);
}

/* Append poiter item ti the Queue. */
static void AppendQueuePtr(Queue *queue, void *item) {
    QueueCell *cell = instance(QueueCell);
    ListCell *lc = instance(ListCell);
    lc->ptr_value = item;
    cell->data = lc;
    cell->next = NULL;
    cell->pres = NULL;
    AppendQueueCell(queue, cell);
}

/* Append item to the Queue. */
void AppendQueue(Queue *queue, void *item) {
    Assert(queue != NULL);
    switch (queue->tag) {
        case NODE_INT:
            AppendQueueInt(queue, *(int *) item);
            break;
        case NODE_BOOL:
            AppendQueueBool(queue, *(bool *) item);
            break;
        case NODE_FLOAT:
            AppendQueueFloat(queue, *(float *) item);
            break;
        case NODE_DOUBLE:
            AppendQueueDouble(queue, *(double *) item);
            break;
        default:
            AppendQueuePtr(queue, item);
            break;
    }
}

/* Delete QueueCell from the Queue. */
static void DeleteQueueCell(Queue *queue, QueueCell *qc) {
    if (queue->head == qc) 
        queue->head = qc->next;

    if (qc->pres != NULL) 
        qc->pres->next = qc->next;
    
    if (qc->next != NULL) 
        qc->next->pres = qc->pres;

    queue->size--;
}

/* Delete int item from the Queue. */
static void DeleteQueueInt(Queue *queue, int item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst_int(qc) == item) {
            DeleteQueueCell(queue, qc);
        }
    }
}

/* Delete bool item from the Queue. */
static void DeleteQueueBool(Queue *queue, bool item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst_bool(qc) == item) {
            DeleteQueueCell(queue, qc);
        }
    }
}


/* Delete float item from the Queue. */
static void DeleteQueueFloat(Queue *queue, float item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst_float(qc) == item) {
            DeleteQueueCell(queue, qc);
        }
    }
}


/* Delete double item from the Queue. */
static void DeleteQueueDouble(Queue *queue, double item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst_double(qc) == item) {
            DeleteQueueCell(queue, qc);
        }
    }
}


/* Delete poiter item from the Queue. */
static void DeleteQueuePtr(Queue *queue, void *item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst(qc) == item) {
            DeleteQueueCell(queue, qc);
        }
    }
}

/* Delete item from the Queue. */
void DeleteQueue(Queue *queue, void *item) {
    Assert(queue != NULL);
    switch (queue->tag) {
        case NODE_INT:
            DeleteQueueInt(queue,  *(int *) item);
            break;
        case NODE_BOOL:
            DeleteQueueBool(queue, *(bool *) item);
            break;
        case NODE_FLOAT:
            DeleteQueueFloat(queue, *(float *) item);
            break;
        case NODE_DOUBLE:
            DeleteQueueDouble(queue, *(double *) item);
            break;
        default:
            DeleteQueuePtr(queue, item);
            break;
    }
}

/* If the int item is the member of queue.  */
bool QueueMemberInt(Queue *queue, int item) {
    QueueCell *qc;
    qforeach (qc, queue) {
        if (qfirst_int(qc) == item) {
            return true;
        }
    }
    return false;
}

/* Free the queue. */
void FreeQueue(Queue *queue) {
    dfree(queue);
}
