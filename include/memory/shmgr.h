#include <stdlib.h>
#include <stdbool.h>
#include "spinlock.h"

typedef struct ShMemFreeEntry {
    size_t  size;
    struct ShMemFreeEntry *next;
} ShMemFreeEntry;

#define SHM_OFFSET MAXALIGN(sizeof(ShMemFreeEntry))

#define SHM_MINBITS       3   /* Smallest chunk size is 8 bytes. */
#define SHM_FREELISTS_NUM 24  /* FreeLists num. */
#define SHM_ALLOC_LIMIT   1 << (SHM_FREELISTS_NUM + SHM_MINBITS - 1)  /* Size of largest chunk. */
#define GET_SIZE_FROM_FREE_LIST_IDX(fdx) \
            ((((Size) 1) << SHM_MINBITS) << (fdx))

#define GET_FREE_ENTRY(ptr)\
    (ShMemFreeEntry *)((char *) ptr - SHM_OFFSET)

#define GET_POINTER(entry)\
    ((void *) ((char *) entry + SHM_OFFSET))
    

void init_mem();
int ShFreeIndex(Size size);
void *shdalloc(size_t size);
void shdfree(void *ptr);
void *shdrealloc(void *ptr, size_t size);
char *shdstrdup(char *str);
