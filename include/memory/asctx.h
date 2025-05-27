#include <stdint.h>
#include "mctx.h"
#include "c.h"

#define ALLOCSET_NUM_FREELISTS  11

typedef struct AllocBlockData *AllocBlock;
typedef struct AllocSetContext *AllocSet;
typedef struct AllocChunkData *AllocChunk;

typedef struct AllocSetContext{
    MemoryContextData header;                       /* Implement standard MemoryContext and must position first */
    AllocBlock blocks;                              /* Header of AllocBlock list. */
    AllocChunk free_list[ALLOCSET_NUM_FREELISTS];   /* Free chunk list. */ 
    uint32_t next_block_size;                       /* Next block size used when allocating new block. */
} AllocSetContext;

typedef struct AllocBlockData {
    AllocSet set;
    AllocBlock pres;
    AllocBlock next;
    char *freeptr;          /* Start of free space of this block. */
    char *endptr;           /* End of free space of this block. */
} AllocBlockData;

typedef struct AllocChunkData {
    AllocChunk next;            /* Next AllocChunk, use in free list. */
    uint64_t   mask;            /* Store details of the chunk. */
} AllocChunkData;

#define ALLOC_MINBITS       3   /* Smallest chunk size is 8 bytes. */
#define ALLOC_FREELISTS_NUM 11  /* FreeLists num. */
#define ALLOC_CHUNK_LIMIT   1 << (ALLOC_FREELISTS_NUM + ALLOC_MINBITS - 1)  /* Size of largest chunk. */

#define ALLOC_SET_CXT_SIZE  sizeof(AllocSetContext)
#define ALLOC_BLOCK_SIZE    sizeof(AllocBlockData)
#define ALLOC_CHUNK_SIZE    sizeof(AllocChunkData)

#define DEFAULT_MAX_BLOCK_SIZE 8 * 1024 * 1024


#define ALLOC_CHUNK_EXTERNAL_BASEBIT        3
#define ALLOC_CHUNK_VALUE_BASEBIT           (ALLOC_CHUNK_EXTERNAL_BASEBIT + 1)
#define ALLOC_CHUNK_BLOCK_OFFSET_BASEBIT    (ALLOC_CHUNK_VALUE_BASEBIT + 30)
#define ALLOC_CHUNK_MAX_VALUE               UINT64CONST(0x3FFFFFFF)
#define ALLOC_CHUNK_MAX_OFFSET              UINT64CONST(0x3FFFFFFF)
#define ALLOC_CHUNK_BLOCK_OFFSET_MASK       UINT64CONST(0x3FFFFFFF)
#define ALLOC_CHUNK_MAGIC                   (UINT64CONST(0xBDEADB858E77BABA) >> ALLOC_CHUNK_VALUE_BASEBIT << ALLOC_CHUNK_VALUE_BASEBIT)


#define KEEPER_ALLOC_BLOCK(set) \
            ((AllocBlockData *) ((char *) set + ALLOC_SET_CXT_SIZE))

#define IS_KEEPER_BLOCK(set, block) \
            (block == (KEEPER_ALLOC_BLOCK(set)))

#define CHUNK_GET_POINTER(chunk) \
            ((void *) ((char *) chunk + ALLOC_CHUNK_SIZE))

#define POINTER_GET_CHUNK(ptr) \
            ((AllocChunkData *) ((char *) ptr - ALLOC_CHUNK_SIZE))

#define CHUNK_EXTERNAL_GET_BLOCK(chunk) \
            ((AllocBlockData *) ((char *) chunk - ALLOC_BLOCK_SIZE))

#define CHUNK_GET_SIZE_FROM_FREE_LIST_IDX(fdx) \
            ((((Size) 1) << ALLOC_MINBITS) << (fdx))

#define CHUNK_IS_EXTERNAL(mask) \
            (mask & (((uint64_t) 1) << ALLOC_CHUNK_EXTERNAL_BASEBIT))

#define CHUNK_CHECK_MAGIC(mask) \
            (ALLOC_CHUNK_MAGIC == (mask >> ALLOC_CHUNK_VALUE_BASEBIT << ALLOC_CHUNK_VALUE_BASEBIT))

#define CHUNK_GET_OFFSET(mask) \
            ((mask >> ALLOC_CHUNK_BLOCK_OFFSET_BASEBIT) & ALLOC_CHUNK_BLOCK_OFFSET_MASK)

#define CHUNK_GET_VALUE(mask) \
            ((mask >> ALLOC_CHUNK_VALUE_BASEBIT) & ALLOC_CHUNK_MAX_VALUE)

/* The implement MemoryContextCreate in AllocSetContext. */
MemoryContext AllocSetMemoryContextCreate(MemoryContext parent, char *name, uint32_t max_block_size);

/* Allocate from AllocSetContext. */
void *AllocSetAlloc(MemoryContext context, Size size);

/* Free in AllocSetContext. */
void AllocSetFree(void *ptr);

/* Reallocate in AllocSetContext. */
void *AllocSetRealloc(void *ptr, Size size);

/* Reset AllocSetContext. */
void AllocSetReset(MemoryContext context);

/* Delete AllocSetContext. */
void AllocSetDelete(MemoryContext context);

