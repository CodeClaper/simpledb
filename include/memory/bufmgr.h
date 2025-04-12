#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "bufpool.h"
#include "rwlock.h"
#include "shmem.h"
#include "utils.h"

#ifndef BUFMGR_H
#define BUFMGR_H

typedef int64_t BlockNum;
typedef int64_t Buffer;

/* Use half of shared memory to store buffer. 
 * So the BUFFER_SLOT_NUM as flowing. */
#define BUFFER_SLOT_NUM  ((SHMEM_SIZE * 2) / (3 * PAGE_SIZE))

/* Buffer tag. */
typedef struct BufferTag {
    char tableName[MAX_TABLE_NAME_LEN];
    BlockNum blockNum;
} BufferTag;

/* BufferDescStatus. */
typedef enum BufferDescStatus {
    EMPTY = 0,
    PINNED,
    UNPINNED
} BufferDescStatus;

/* Buffer Desc. */
typedef struct BufferDesc {
    BufferTag tag;              /* Buffer tag. */
    Buffer buffer;              /* Buffer value, corresponding to page number. */
    BufferDescStatus status;    /* Buffer Desc status. */
    volatile int refcount;      /* Reference count. */
    volatile int usage_count;   /* Usage count. */
    RWLockEntry lock;           /* RW lock. */
    s_lock      io_lock;        /* IO lock.*/
} BufferDesc;

typedef struct VictimIndex {
    volatile Index index;
    s_lock lock;
} VictimIndex;

/* Return if both BufferTags equals. */
static inline bool BufferTagEquals(BufferTag *tag1, BufferTag *tag2) {
    return (tag1->blockNum == tag2->blockNum) 
                && streq(tag1->tableName, tag2->tableName);
}

/* Return if the BufferTag is empty. */
static inline bool BufferTagEmpty(BufferTag tag) {
    return is_null(tag.tableName) && tag.blockNum == 0;
}

/* Make the BufferTag empty. */
static inline void MakeBufferTagEmpty(BufferTag *tag) {
    tag->blockNum = 0;
    memset(tag->tableName, 0, MAX_TABLE_NAME_LEN);
}

/* Init BufMgr. */
void InitBufMgr();

/* Get the BufferDesc. */
BufferDesc *GetBufferDesc(Buffer buffer);

/* Read Buffer.
 * Get shared buffer data via Buffer value. 
 * */
Buffer ReadBuffer(Table *table, BlockNum blockNum);

/* Read Buffer Inner.
 * Get shared buffer data via Buffer value. 
 * */
Buffer ReadBufferInner(char *table_name, BlockNum blockNum);

/* Release Buffer.
 * Release Buffer after using. 
 * And the function is called aftert ReadBuffer. 
 * */
void ReleaseBuffer(Buffer buffer);

/* Release Buffer Inner.
 * Release Buffer after using. 
 * And the function is called aftert ReadBuffer. 
 * */
void ReleaseBufferInner(Buffer buffer);

/* Upgrade Lock Buffer. */
void UpgradeLockBuffer(Buffer buffer);

/* Downgrade Lock Buffer. */
void DowngradeLockBuffer(Buffer buffer);

/* LocK Buffer. 
 * Try to acquire the exclusive content lock in BufferDesc.
 * */
void LockBuffer(Buffer buffer, RWLockMode mode);

/* Unlock Buffer
 * Unlock the exclusive content lock in BufferDesc.
 * */
void UnlockBuffer(Buffer buffer);

/* Get Buffer page. */
void *GetBufferPage(Buffer buffer);

/* Get Buffer page copy. */
void *GetBufferPageCopy(Buffer buffer);

/* Pin the buffer. */
void PinBuffer(BufferDesc *desc);

/* Unpin the buffer. */
void UnpinBuffer(BufferDesc *desc);

/* Make Buffer dirty. */
void MakeBufferDirty(Buffer buffer);

/* Get Lock Mode. */
RWLockMode GetLockModeBuffer(Buffer buffer);

#endif
