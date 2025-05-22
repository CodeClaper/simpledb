/***************************** Shared Memory Module *****************************************
 * Auth:        JerryZhou
 * Created:     2024/10/24
 * Modify:      2024/10/25
 * Locataion:   src/memory/asctx.c
 * 
*********************************************************************************************/

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "asctx.h"
#include "mctx.h"
#include "mmgr.h"
#include "shmem.h"
#include "bit.h"
#include "utils.h"
#include "asserts.h"

void *AllocSetAllocNewBlock(MemoryContext context, Size chksize);
void *AllocSetAllocChunkFromBlock(MemoryContext context, AllocBlock block, Size chksize);

/* Index of AllocSet free size 
 * --------------------------
 * At this pointer we must compute ceil(log2(size >> ALLOC_MINBITS))
 * This is the same as
 *      leftmost_32_pos((size - 1) >> ALLOC_MINBITS) + 1
 * or equivalently
 *      leftmost_32_pos(size - 1) - ALLOC_MINBITS + 1
 * -------------------------
 * */
static inline int AllocSetFreeIndex(Size size) {
    int idx = size > (1 << ALLOC_MINBITS) 
            ? leftmost_32_pos(size -1) - ALLOC_MINBITS + 1
            : 0;
    Assert(idx < ALLOC_FREELISTS_NUM);
    return idx;
}

/* The implement MemoryContextCreate in AllocSetContext. */
MemoryContext AllocSetMemoryContextCreate(MemoryContext parent, char *name, uint32_t max_block_size) {
    Size size;
    AllocSet set;
    AllocBlock block;

    /* Block size at least include AllocSetContext size + AllocBlock size + at lest one AllocChunk size.*/
    size = ALLOC_SET_CXT_SIZE + ALLOC_BLOCK_SIZE + ALLOC_CHUNK_SIZE;
    size = max_size(size, max_block_size);
    size = MAXALIGN(size);
    
    Assert(in_local_memory());
    /* Alloc from system. */
    set = (AllocSet) malloc(size);
    if (set == NULL) {
        perror("Malloc fail.");
        exit(1);
    }
    Assert(!shmem_addr_valid(set));

    /* Assignment. */
    block = KEEPER_ALLOC_BLOCK(set);
    block->set = set;
    block->pres = NULL;
    block->next = NULL;
    block->freeptr = ((char *) block) + ALLOC_BLOCK_SIZE;
    block->endptr = ((char *) set) + size;

    set->blocks = block;
    set->next_block_size = max_block_size;

    /* Init the free list*/
    memset(set->free_list, 0, sizeof(set->free_list));

    MemoryContextCreate(
        (MemoryContext) set, 
        parent, 
        name,
        ALLOC_SET_CTX, 
        ALLOC_SET_ID
    );

    set->header.allocated_size =  size;

    return (MemoryContext) set;
}

/* Store details into chunk mask. 
 * Details include:
 * offset: offset to block.
 * value: chunk allocated size.
 * */
static inline void AllocChunkSetMask(AllocChunk chunk, AllocBlock block, Size value) {
    Size offset = (char *) chunk - (char *) block;
    Assert(offset <= ALLOC_CHUNK_MAX_OFFSET);
    chunk->mask = (((uint64_t) offset) << ALLOC_CHUNK_BLOCK_OFFSET_BASEBIT) | ((uint64_t) value) << ALLOC_CHUNK_VALUE_BASEBIT;
}

/* Set chunk as an externally managed chunk. */
static inline void AllocChunkSetMaskExternal(AllocChunk chunk) {
    chunk->mask = ALLOC_CHUNK_MAGIC | (((uint64_t) 1) << ALLOC_CHUNK_EXTERNAL_BASEBIT);
}

/* Return true if chunk is marked as external. */
static inline bool AllocChunkIsExternal(AllocChunk chunk) {
    Assert(!CHUNK_IS_EXTERNAL(chunk->mask) || CHUNK_CHECK_MAGIC(chunk->mask));
    return CHUNK_IS_EXTERNAL(chunk->mask);
}

/* Get AllocBlock by AllocChunk. */
static inline AllocBlock AllocChunkGetBlock(AllocChunk chunk) {
    Assert(!CHUNK_IS_EXTERNAL(chunk->mask));
    return (AllocBlock) ((char *) chunk - CHUNK_GET_OFFSET(chunk->mask));
}

/* Allocate for large memory.
 * That allocates en entire block for the chunk. */
static void *AllocSetAllocLarge(MemoryContext context, Size size) {
    AllocSet set = (AllocSet) context;
    Size blk_size;
    AllocBlock block;
    AllocChunk chunk;
    
    blk_size = MAXALIGN(size + ALLOC_BLOCK_SIZE + ALLOC_CHUNK_SIZE);
    block = (AllocBlock) malloc(blk_size);
    if (block == NULL) {
        perror("Malloc fail.");
        exit(1);
    }

    context->allocated_size += blk_size;
    block->set = set;
    block->freeptr = block->endptr = ((char *) block) + blk_size;
    chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCK_SIZE);
    /* Mark the Chunk as externally managed. */
    AllocChunkSetMaskExternal(chunk);

    if (set->blocks != NULL) {
        block->pres = set->blocks;
        block->next = set->blocks->next;
        if (block->next != NULL)
            block->next->pres = block;
        set->blocks->next = block;
    } else {
        block->pres = NULL;
        block->next = NULL;
        set->blocks = block;
    }

    return CHUNK_GET_POINTER(chunk);
}

/* Allocate from AllocSetContext. */
void *AllocSetAlloc(MemoryContext context, Size size) {
    Size chksize, freesize;
    int fdx;
    AllocChunk chunk;

    if (size > ALLOC_CHUNK_LIMIT)
        return AllocSetAllocLarge(context, size);

    AllocSet set = (AllocSet) context;
    Assert(set->blocks);
    AllocBlock block = set->blocks;

    /* Find there is enough chunk in FreeList. */
    fdx = AllocSetFreeIndex(size);
    chunk = set->free_list[fdx];
    if (non_null(chunk)) {
        set->free_list[fdx] = chunk->next;
        return CHUNK_GET_POINTER(chunk);
    }

    chksize = CHUNK_GET_SIZE_FROM_FREE_LIST_IDX(fdx);
    Assert(chksize >= size);

    /* Get block free space size. 
     * If it has enough free space, allocate from current block. 
     * Otherwise, generate new block and allocate from that. */
    freesize = block->endptr - block->freeptr;
    if (freesize < chksize + ALLOC_CHUNK_SIZE) 
        return AllocSetAllocNewBlock(context, chksize);

    /* There is enough space on the block. */
    return AllocSetAllocChunkFromBlock(context, block, chksize);
}


/* Allocate Chunk from Current Block. */
void *AllocSetAllocChunkFromBlock(MemoryContext context, AllocBlock block, Size chksize) {
    AllocChunk chunk = (AllocChunk) block->freeptr;
    block->freeptr += (chksize + ALLOC_CHUNK_SIZE);
    Assert(block->freeptr <= block->endptr);
    chunk->next = NULL;
    AllocChunkSetMask(chunk, block, chksize);
    return CHUNK_GET_POINTER(chunk);
}


/* Allocate from New Block. */
void *AllocSetAllocNewBlock(MemoryContext context, Size chksize) {
    Size blk_size;

    AllocSet set = (AllocSet) context;
    Assert(set->blocks != NULL); 

    blk_size = set->next_block_size;
    AllocBlock block = (AllocBlock) malloc(blk_size);
    if (block) {
        perror("Malloc fail.");
        exit(1);
    }

    block->freeptr = ((char *) block) + ALLOC_BLOCK_SIZE;
    block->endptr = ((char *) block) + blk_size;
    block->set = set;
    block->next = set->blocks;
    block->pres = NULL;
    
    if (block->next)
        block->next->pres = block;
    set->blocks = block;

    context->allocated_size += blk_size;

    return AllocSetAllocChunkFromBlock(context, block, chksize);
}

/* Free in AllocSetContext. */
void AllocSetFree(void *ptr) {
    Assert(ptr);
    AllocSet set;
    AllocChunk chunk = POINTER_GET_CHUNK(ptr);

    if (AllocChunkIsExternal(chunk)) {
        AllocBlock block = CHUNK_EXTERNAL_GET_BLOCK(chunk);
        set = block->set;
        if (block->pres)
            block->pres->next = block->next;
        else
            set->blocks = block->next;
        if (block->next)
            block->next->pres = block->pres;
        set->header.allocated_size -= block->endptr - ((char *) block);
        free(block);
    } else {
        AllocBlock block = AllocChunkGetBlock(chunk); 
        set = block->set;
        int fdx = AllocSetFreeIndex(CHUNK_GET_VALUE(chunk->mask));
        chunk->next = set->free_list[fdx];
        set->free_list[fdx] = chunk;
    }
}

/* Reallocate in AllocSetContext. */
void *AllocSetRealloc(void *ptr, Size size) {
    AllocChunk chunk = POINTER_GET_CHUNK(ptr);
    AllocBlock block;
    AllocSet set;
    Size old_size;

    /* Way to external chunk. */
    if (AllocChunkIsExternal(chunk)) {
        Size blksize;
        Size oldblksize;

        block = CHUNK_EXTERNAL_GET_BLOCK(chunk);
        set = block->set;
        blksize = MAXALIGN(size + ALLOC_BLOCK_SIZE + ALLOC_CHUNK_SIZE);
        oldblksize = block->endptr - ((char *) block);

        block = realloc(block, blksize);
        if (is_null(block)) {
            fprintf(stderr, "Out of memory.");
            exit(1);
        }

        set->header.allocated_size -= oldblksize;
        set->header.allocated_size += blksize;

        block->freeptr = block->endptr = ((char *) block) + blksize;
        chunk = (AllocChunkData *) (((char *) block) + ALLOC_BLOCK_SIZE);

        if (block->pres)
            block->pres->next = block;
        else
            set->blocks = block;
        if (block->next)
            block->next->pres = block;

        return CHUNK_GET_POINTER(chunk);
    }

    /* Way to normal chunk. */
    block = AllocChunkGetBlock(chunk);
    set = block->set;
    old_size = CHUNK_GET_VALUE(chunk->mask);

    if (old_size >= size) 
        return ptr; 
    else {
        void *nptr = AllocSetAlloc((MemoryContext) set, size);
        memcpy(nptr, ptr, old_size);
        AllocSetFree(ptr);
        return nptr;
    }
}

/* Reset AllocSetContext. 
 * Free all allocated chunk memory which is allocated in the given set.
 * To simply, we directly free the blocks, but keep the first block which 
 * genrated when Memory Context created.
 * */
void AllocSetReset(MemoryContext context) {
    AllocSet set = (AllocSet) context;
    Size keepersize = KEEPER_ALLOC_BLOCK(set)->endptr - ((char *) set);
    AllocBlock block = set->blocks;

    /* Clean the free list*/
    memset(set->free_list, 0, sizeof(set->free_list));

    /* Reset first block as header. */
    set->blocks = KEEPER_ALLOC_BLOCK(set);

    while (block != NULL) {
        AllocBlock next = block->next;
        if (IS_KEEPER_BLOCK(set, block)) {
            // Way to handle first block.
            char *start = ((char *) block) + ALLOC_BLOCK_SIZE;
            block->freeptr = start;
            block->next = NULL;
            block->pres = NULL;
        } else {
            // Way to handle other block.
            context->allocated_size -= block->endptr - ((char *) block);
            free(block);
        }
        block = next;
    }

    /* Just check, if reset is successful. */
    Assert(context->allocated_size == keepersize);
}

/* Delete AllocSetContext. 
 * Free all memory which is allocated in the given set.
 * Unlike AllocSetReset, this will free free all resource include the set itself.
 * */
void AllocSetDelete(MemoryContext context) {
    AllocSetReset(context);
    free((AllocSet) context);
    context = NULL;
}
