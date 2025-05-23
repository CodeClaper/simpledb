#include <stdbool.h>
#include <stdlib.h>

#ifndef MMGR_H
#define MMGR_H

typedef enum MemType {
    MEM_LOCAL = 0,
    MEM_SHARED
} MemType;

typedef struct MemTypeRecord {
    MemType type;
    pthread_t *worker;
} MemTypeRecord;

typedef struct MemMethods {
    void *(*dalloc)(size_t size);
    void (*dfree)(void *ptr);
    void *(*drealloc)(void *ptr, size_t size);
    char *(*dstrdup)(char *str);
} MemMethods;

#define instance(_type_)  ((_type_ *) dalloc(sizeof(_type_)))


#define LMENTRY_OFFSET sizeof(LoMemEntry)


/* Define as the maximum alignment requirement of any C data type. */
#define MAXIMUM_ALIGNOF 8

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define MAX_MALLOC_SIZE (1024 * 1024 * 1024 * 2L)


void RegisterWorkers(pthread_t workers[], int workerNum);

/* Swith Shared Memory. */
void switch_shared();

/* Switch Local Memory. */
void switch_local();

/* In Shared memory. */
bool in_shared_memory();

/* In Local memory. */
bool in_local_memory();

/* Allocate memory. */
void *dalloc(size_t size);

/* Free memory. */
void dfree(void *ptr);

/* Reallocate memory. */
void *drealloc(void *ptr, size_t size);

/* Strdup. */
char *dstrdup(char *str);

#endif
