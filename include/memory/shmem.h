#include <stdlib.h>
#include <stdbool.h>

#ifndef SHMEM_H
#define SHMEM_H

#define SHMEM_SIZE (1024 * 1024 * 1024 * 3L)  // shared memory 3GB. 

typedef struct ShmemHeader {
    size_t total_size;
    volatile size_t offset;
} ShmemHeader;

/* Init the shared memory. */
void init_shmem();

/* Allocate memory in Shmem. */
void *shmem_alloc(size_t size);

/* Check if addess is valid shared memory. */
bool shmem_addr_valid(void *ptr);

#endif
