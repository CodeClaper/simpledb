#include <stdint.h>
#include "list.h"
#include "c.h"

typedef struct Queue {
    NodeTag tag;
    volatile Size size;
    ListCell *data;
    struct Queue *pres;
    struct Queue *next;
} Queue;


Queue *create_queue(NodeTag tag);
