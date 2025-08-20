/***************************** Shared Memory Mamnager Module *********************************
 * Auth:            JerryZhou 
 * Created:         2024/11/16 
 * Modify:          2024/11/16
 * Locataion:       src/memory/shdmem.c
 * Description:     This is the implement of mem for manipulating shared memory.
 *********************************************************************************************/
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "shmgr.h"
#include "shmem.h"
#include "mmgr.h"
#include "utils.h"
#include "bit.h"

static void try_shdfree(void *ptr);

static ShMemFreeEntry *sh_free_list[SHM_FREELISTS_NUM];
static s_lock *shlock;

int ShFreeIndex(Size size) {
    int idx = size > (1 << SHM_MINBITS) 
            ? leftmost_32_pos(size -1) - SHM_MINBITS + 1
            : 0;
    return idx;
}


/* Init mem. */
void init_mem() {
    shlock = shmem_alloc(sizeof(s_lock));
    init_spin_lock(shlock);
    for (int i = 0; i < SHM_FREELISTS_NUM; i++) {
        sh_free_list[i] = NULL;
    }
}

/* Alloc large memory for share. */
void *shdalloc_large(size_t size) {
    void *ptr;

    void *nptr = shmem_alloc(size + SHM_OFFSET);
    ShMemFreeEntry entry = { 
        .size = size,
        .next = NULL
    };
    memcpy(nptr, &entry, SHM_OFFSET);
    ptr = nptr + SHM_OFFSET;

    return ptr;
}

/* Alloc for share. 
 * ----------------
 * If allocate large memory, call the shdalloc_large.
 * If allocate normal memory, it will record in the sh_free_list.
 * */
void *shdalloc(size_t size) {
    int fdx;
    void *ptr;
    ShMemFreeEntry *entry;

    Assert(size > 0);
    if (size > SHM_ALLOC_LIMIT)
        return shdalloc_large(size);

    Assert(size < SHM_ALLOC_LIMIT);

    /* First reuse in free list, 
     * if missing, allocate new one.*/
    acquire_spin_lock(shlock);
    fdx = ShFreeIndex(size);
    Assert(fdx <= SHM_FREELISTS_NUM);
    entry = sh_free_list[fdx];
    if (size < SHM_ALLOC_LIMIT && entry != NULL) {
        if (size > entry->size)
            Assert(entry->size >= size);
        sh_free_list[fdx] = entry->next;
        ptr = GET_POINTER(entry);
    } else {
        size = GET_SIZE_FROM_FREE_LIST_IDX(fdx);
        void *nptr = shmem_alloc(size + SHM_OFFSET);
        ShMemFreeEntry entry = { 
            .size = size,
            .next = NULL
        };
        memcpy(nptr, &entry, SHM_OFFSET);
        ptr = nptr + SHM_OFFSET;
    }
    release_spin_lock(shlock);

    /* Initialise data. */
    memset(ptr, 0, size);

    return ptr;
}

/* Free for share. */
void shdfree(void *ptr) {
    int fdx;
    Assert(ptr);
    Assert(shmem_addr_valid(ptr));

    acquire_spin_lock(shlock);
    ShMemFreeEntry *entry = GET_FREE_ENTRY(ptr);
    Assert(entry != NULL);
    Assert(entry->size <= SHM_ALLOC_LIMIT);

    fdx = ShFreeIndex(entry->size);
    entry->next = sh_free_list[fdx];
    sh_free_list[fdx] = entry;
    release_spin_lock(shlock);
}

/* Realloc for share. */
void *shdrealloc(void *ptr, size_t size) {
    Assert(ptr);

    ShMemFreeEntry *entry = ptr - SHM_OFFSET;
    void *new_ptr = shdalloc(size);
    memcpy(new_ptr, ptr, min_size(entry->size, size));
    try_shdfree(ptr);
    return new_ptr;
}

/* Strdup for share. */
char *shdstrdup(char *str) {
    if (IsNull(str))
        return NULL;
    size_t size = strlen(str) + 1;
    void *new_str = shdalloc(size);
    memcpy(new_str, str, strlen(str));
    return new_str;
}


/* Try free for share. */
static void try_shdfree(void *ptr) {
    if (shmem_addr_valid(ptr))
        shdfree(ptr);
}

