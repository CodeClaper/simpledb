
typedef void (*PARALLEL_TASK) (void *args);

/* The maxinum worker num. */
#define MAX_WORKER_NUM 100

typedef enum ComputeMode {
    NORMAL_COMPUTE,
    PARALLEL_COMPUTE
} ComputeMode;

typedef struct TaskEntry {
    PARALLEL_TASK task;
    void *taskArg;
    struct TaskEntry *next;
} TaskEntry;

#define COMPUTE_IN_PARALL (GetComputeMode() == PARALLEL_COMPUTE) 

/* Get current ComputeMode. */
ComputeMode GetComputeMode();

/* Parallel computing. */
void ParallelCompute(int workerNum, int taskNum, PARALLEL_TASK task, void *taskArgs[]);

