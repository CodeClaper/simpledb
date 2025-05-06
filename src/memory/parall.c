#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "data.h"
#include "parall.h"
#include "c.h"
#include "mmgr.h"
#include "mctx.h"
#include "log.h"
#include "spinlock.h"

/* 
 * ComputeMode, 
 * default is NORMAL_COMPUTE. 
 */
static ComputeMode currentMode = NORMAL_COMPUTE;

/*
 * Tassk queue.
 */
static TaskEntry *taskQueue;

/* 
 * Queue lock. 
 */
static s_lock qLock = SPIN_UN_LOCKED_STATUS;

/* Get current ComputeMode. */
inline ComputeMode GetComputeMode() {
    return currentMode;
}

static inline ComputeMode SwitchComputeMode(ComputeMode newMode) {
    ComputeMode old = currentMode;
    currentMode = newMode;
    return old;
}

static TaskEntry *GetTaskQueueTail() {
    TaskEntry *task = taskQueue;
    while (task->next != NULL) {
        task = task->next;
    }
    return task;
}

static void RegisterTaskQueue(PARALLEL_TASK task, void *taskArg) {
    TaskEntry *taskEntry = instance(TaskEntry);
    taskEntry->task = task;
    taskEntry->taskArg = taskArg;
    taskEntry->next = NULL;
    if (taskQueue == NULL)
        taskQueue = taskEntry;
    else {
        TaskEntry *tail = GetTaskQueueTail();
        tail->next = taskEntry;
    }
}

static TaskEntry *AssignTask() {
    acquire_spin_lock(&qLock);
    if (taskQueue == NULL) {
        release_spin_lock(&qLock);
        return NULL;
    }
    else {
        TaskEntry *task = taskQueue;
        taskQueue = taskQueue->next;
        release_spin_lock(&qLock);
        return task;
    }
}

static void TaskProxy() {
    TaskEntry *task;
    while ((task = AssignTask()) != NULL) {
        task->task(task->taskArg);
    }
}

/* Parallel computing. */
void ParallelCompute(int workerNum, int taskNum, PARALLEL_TASK task, void *taskArgs[]) {
    Assert(workerNum <= MAX_WORKER_NUM);
    
    db_log(DEBUGER, "Start parall computing, %d workers with %d tasks.", 
           workerNum, taskNum);

    pthread_t workers[workerNum];

    /* Register task. */
    for (int i = 0; i < taskNum; i++) {
        RegisterTaskQueue(task, taskArgs[i]);
    }

    RegisterWorkers(workers, workerNum);
    RegisterContextRecorders(workerNum,  workers);

    /* Switch to PARALLEL_COMPUTE mode. */
    ComputeMode oldMode = SwitchComputeMode(PARALLEL_COMPUTE);

    /* Assign the task. */
    for (int i = 0; i < workerNum; i++) {
        pthread_create(&workers[i], NULL, (void *)&TaskProxy, NULL);
    }

    /* Wait all workers. */
    for (int i = 0; i < workerNum; i++) {
        pthread_join(workers[i], NULL);
    }
    
    SwitchComputeMode(oldMode);

    db_log(DEBUGER, "End parall computing.");
}

