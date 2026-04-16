#include <stdlib.h>
#include <stdbool.h>
#include "defs.h"

#ifndef SHMEM_H
#define SHMEM_H

/* Default share size. 
 * If use not define size of share memory, use this value. */
#define SHMEM_SIZE (1024 * 1024 * 1024 * 1L * (conf->share_memory_size)) 

typedef struct ShmemHeader {
    size_t total_size;
    volatile size_t offset;
} ShmemHeader;

void init_shmem();
void *shmem_alloc(size_t size);
bool shmem_addr_valid(void *ptr);
void destroy_shmem();

#endif
