/***************************** Shared Memory Module *****************************************
* Auth:        JerryZhou
* Created:     2024/08/13
* Modify:      2024/08/13
* Locataion:   src/memory/shmem.c
* Description: The shared memory manager is a component of IPC (Inter process commnuication).
* The shared memory is used to store table cache, table reg and transaction data.
*********************************************************************************************
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <unistd.h>
#include "shmem.h"
#include "spinlock.h"
#include "utils.h"

static ShmemHeader *shmrd;
static s_lock* lock;

static void *BaseEnd;
static void *BaseStart;

static void create_shmem();
static void create_shmem_lock();
static void *shmem_alloc_inner_unlock(size_t size);

/* Init the shared memory. */
void init_shmem() {
    create_shmem();
    create_shmem_lock();
}

/* Create Shmem. */
static void create_shmem() {
    ShmemHeader init_shmrd;
    void *shm_ptr = mmap(NULL, SHMEM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm_ptr == MAP_FAILED) {
        perror("Try to create shared memory fail.");
        exit(1);
    }
    bzero(shm_ptr, SHMEM_SIZE);
        
    /* Init. */
    init_shmrd.total_size = SHMEM_SIZE;
    init_shmrd.offset = sizeof(ShmemHeader);
    
    memcpy(shm_ptr, &init_shmrd, sizeof(ShmemHeader));
    shmrd = shm_ptr;

    BaseStart = (void *) shmrd;
    BaseEnd = ((char *) shmrd) + shmrd->total_size;
}

/* Create spin lock */
static void create_shmem_lock() {
    lock = shmem_alloc_inner_unlock(sizeof(s_lock));
    init_spin_lock(lock);
}

/* Allocate memory in Shmem for inner. 
 * And it only work befor shmem lock ready. */
static void *shmem_alloc_inner_unlock(size_t size) {
    void *ptr;

    Assert(shmrd);

    size_t current_offset = shmrd->offset + size;
    if (current_offset <= shmrd->total_size) 
    {
        ptr = (void *) ((char *) shmrd + shmrd->offset);   
        shmrd->offset = current_offset;
    }
    else 
        ptr = NULL;

    return ptr;
}

/* Allocate memory in Shmem for inner. */
static void *shmem_alloc_inner(size_t size) {
    Assert(shmrd);
    void *ptr;

    acquire_spin_lock(lock);

    size_t current_offset = shmrd->offset + size;
    if (current_offset <= shmrd->total_size) 
    {
        ptr = (void *) ((char *) shmrd + shmrd->offset);   
        shmrd->offset = current_offset;
    }
    else 
        ptr = NULL;

    release_spin_lock(lock);

    return ptr;
}

/* Allocate memory in Shmem. */
void *shmem_alloc(size_t size) {
    void *ptr = shmem_alloc_inner(size);
    if (is_null(ptr)) {
        perror("Out of shared memory.");
        exit(1);
    }
    return ptr;
}

/* Check if addess is valid shared memory. */
bool shmem_addr_valid(void *ptr) {
    return (ptr >= BaseStart) && (ptr < BaseEnd);
}
