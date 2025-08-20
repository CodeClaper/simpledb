/***************************** Local memory Mangager ****************************************** 
 * Auth:            JerryZhou 
 * Created:         2024/11/16 
 * Modify:          2024/11/16
 * Locataion:       src/memory/lodmem.c
 * Description:     This is the implement of mem for local memory.
 *********************************************************************************************/
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "lomgr.h"
#include "utils.h"
#include "mctx.h"
#include "spinlock.h"
#include "shmem.h"

/* Alloc for local. */
void *lodalloc(size_t size) {
    void *ptr;
    ptr = MemoryContextAlloc(size);
    Assert(ptr != NULL);
    memset(ptr, 0, size);
    return ptr;
}

/* Free for local. */
void lofree(void *ptr) {
    Assert(!shmem_addr_valid(ptr));
    MemoryContextFree(ptr);
}

/* Realloc for local. */
void *lodrealloc(void *ptr, size_t size) {
    void *newptr = NULL;
    Assert(!shmem_addr_valid(ptr));
    newptr = MemoryContextRealloc(ptr, size);
    return newptr;
}

/* Strdup for local. */
char *lodstrdup(char *str) {
    char *newStr = NULL;
    if (IsNull(str))
        return newStr;
    newStr = MemoryContextStrdup(str);
    return newStr;
}
