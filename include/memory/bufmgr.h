#include <stdbool.h>
#include <stdint.h>
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
    Oid oid;
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

typedef struct VictimController {
    volatile Index index;
    s_lock lock;
} VictimController;

/* Return if both BufferTags equals. */
static inline bool BufferTagEquals(BufferTag *tag1, BufferTag *tag2) {
    return (tag1->blockNum == tag2->blockNum) 
                && (tag1->oid == tag2->oid);
}

/* Init BufMgr. */
void InitBufMgr();

/* Get the BufferDesc. */
BufferDesc *GetBufferDesc(Buffer buffer);

/* Read Buffer. */
Buffer ReadBuffer(Oid oid, BlockNum blockNum);

void ReleaseBuffer(Buffer buffer);
void UpgradeLockBuffer(Buffer buffer);
void DowngradeLockBuffer(Buffer buffer);
void LockBuffer(Buffer buffer, RWLockMode mode);
void UnlockBuffer(Buffer buffer);
void *GetBufferPage(Buffer buffer);
void *GetBufferPageCopy(Buffer buffer);
void PinBuffer(BufferDesc *desc);
void PinBufferInner(BufferDesc *desc);
void UnpinBuffer(BufferDesc *desc);
void MakeBufferDirty(Buffer buffer);
void MakeBufferNormal(Buffer buffer);
RWLockMode GetLockModeBuffer(Buffer buffer);
List *GetLockedBufferDescList();

#endif
