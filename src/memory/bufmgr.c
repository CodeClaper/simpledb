/***************************************** Buffer Manager Module ****************************************
 * Auth:        JerryZhou
 * Created:     2024/12/04
 * Modify:      2024/12/04
 * Locataion:   src/memory/bufmgr.c
 * Description: The buffer manager supports the way to manipulate the page data.
 * Not recommand that you get page data via the pager manager direactly, but the buffer manager indireactly.
 * This module aims to avoid page data update conficting and minimize the impact on read and write performance 
 * in concurrent scenarios.
 *
 * -----------------
 * Basic routine:
 *   (1) Lockless routine:
 *       ReadBuffer -> ReleaseBuffer
 *   (2) Lock routine:
 *       ReadBuffer -> LockBuffer -> UnlockBuffer -> ReleaseBuffer
 *
 * -----------------
 * Lockless routine
 * Get data via ReadBuffer and manipulate without lock. These lockless manipulations includes:
 *   (1) update row
 *   (2) append row witout moving page data.
 *
 * It`s not neccessary to use locking mechanisma for manipulation (1) and (2) under the MVVC mechanism.
 *
 * When we get data via ReadBuffer and the refcount will increase. The refcount is import for
 * LockBuffer. After using the page data, must to release the data via ReadBuffer. And the 
 * refcount will descrease.
 *
 * ---------------
 * Lock routine:
 * Get data via ReadBuffer and then if you want to manipulate the data exclusivly, call LockBuffer. 
 * These lock manipulations includes:
 * (1) Move page data.
 * (2) Split page.
 *
 * When calling LockBuffer, it maybe block until satisfing two condition:
 * (1) Acquire the exclusive lock.
 * (2) The refcount to be one (only itself).
 * It release the exclusive content lock via UnlockBuffer.
 ************************************************************************************************************
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include "bufmgr.h"
#include "asserts.h"
#include "mmgr.h"
#include "ltree.h"
#include "buftable.h"
#include "bufpool.h"
#include "c.h"
#include "table.h"
#include "pager.h"
#include "copy.h"
#include "log.h"

/*
 * BufferDesc table. 
 */
static BufferDesc *bDescTable;

/*
 * Next Victim index.
 */
static VictimIndex *victimIndex;

/* Create BufferDesc. */
void CreateBufferDescTable() {
    Size size = BUFFER_SLOT_NUM;
    switch_shared();

    /* Init BDescTable. */
    bDescTable = dalloc(sizeof(BufferDesc) * size);
    for (Index i = 0; i < size; i++) {
        BufferDesc *desc = (BufferDesc *)(bDescTable + i);
        desc->buffer = i;
        desc->status = EMPTY;
        desc->refcount = 0;
        desc->usage_count = 0;
        InitRWlock(&desc->lock);
        init_spin_lock(&desc->io_lock);
    }
    
    /* Init VictimIndex. */
    victimIndex = instance(VictimIndex); 
    victimIndex->index = 0;
    init_spin_lock(&victimIndex->lock);

    switch_local();
}


/* Init BufMgr. */
void InitBufMgr() {
    CreateBufferTable();
    CreateBufferDescTable();
    CreateBufferPool();
}

/* Pin the buffer. */
void PinBuffer(BufferDesc *desc) {
    acquire_spin_lock(&desc->io_lock);
    desc->status = PINNED;
    desc->usage_count++;
    desc->refcount++;
    release_spin_lock(&desc->io_lock);
}

/* Unpin the buffer. */
void UnpinBuffer(BufferDesc *desc) {
    acquire_spin_lock(&desc->io_lock);
    desc->refcount--;
    desc->status = UNPINNED;
    Assert(desc->refcount >= 0);
    release_spin_lock(&desc->io_lock);
}

/* Next Victim index. */
static inline Index NextVictimIndex() {
    acquire_spin_lock(&victimIndex->lock);
    Index current = victimIndex->index;
    if (victimIndex->index >= BUFFER_SLOT_NUM - 1) 
        victimIndex->index = 0;
    else
        victimIndex->index++;
    release_spin_lock(&victimIndex->lock);
    return current;
}

/* Get the BufferDesc. */
inline BufferDesc *GetBufferDesc(Buffer buffer) {
    Assert(buffer < BUFFER_SLOT_NUM);
    return (BufferDesc *)(bDescTable + buffer);
}

/* Loop Find BufferDesc when missing in BDescTable. */
static BufferDesc *LoopFindBufferDesc(BufferTag *tag) {
    Index vindex;
    BufferDesc *desc;
    
    /* Loop the circle to lookup the free table buffer. */
    forever {
        vindex = NextVictimIndex();
        desc = GetBufferDesc(vindex);
        if (desc->status == EMPTY) {
            PinBuffer(desc);
            break;
        } else if (desc->status == UNPINNED) {
            if (desc->usage_count == 0) {
                PinBuffer(desc);
                /* Write the old buffer to storage. */
                BufferWriteBlock(desc->buffer);
                /* Delete Old Buffer Table Entry. */
                DeleteBufferTableEntry(&desc->tag);
                break;
            }
            desc->usage_count--;
        }
    }

    return desc;
}

/* Load new BufferDesc. 
 * -------------------
 * The buffer missing in the buffer table.
 * */
static BufferDesc *LoadNewBufferDesc(BufferTag *tag) {
    Buffer buffer;
    BufferTableEntrySlot *slot;
    BufferDesc *desc;

    /* Get slot to acquire the rwlock. */
    slot = GetBufferTableSlot(tag);

    /* Acquire the rwlock in exclusive mode. */
    AcquireRWlock(slot->lock, RW_WRITER);

    /* Double check. */
    buffer = LookupBufferTableWithoutLock(tag);
    if (buffer >= 0) {
        desc = GetBufferDesc(buffer);
        /* Maybe the buffer desc has unpinned and reused, 
         * neccessary to check the tag if still.*/
        if (BufferTagEquals(tag, &desc->tag)) {
            PinBuffer(desc);
            ReleaseRWlock(slot->lock);
            return desc;
        }
    }

    /* Loop the circle to find access buffer desc. */
    desc = LoopFindBufferDesc(tag);
    
    /* Load the desired page data from storage. */
    BufferReadBlock(tag, desc->buffer);

    /* Update the tag. */
    desc->tag.blockNum = tag->blockNum;
    memcpy(desc->tag.tableName, tag->tableName, MAX_TABLE_NAME_LEN);

    /* Insert new Buffer Table Entry. */
    InsertBufferTableEntry(tag, desc->buffer);

    /* Release the rwlock. */
    ReleaseRWlock(slot->lock);

    return desc;
}


/* Read Buffer.
 * Get shared buffer data via Buffer value. */
Buffer ReadBuffer(Table *table, BlockNum blockNum) {
    return ReadBufferInner(
        get_table_name(table), 
        blockNum
    );
}

/* Read Buffer.
 * Get shared buffer data via Buffer value. */
Buffer ReadBufferInner(char *table_name, BlockNum blockNum) {
    Buffer buffer;
    BufferTag tag;
    BufferDesc *desc;
    
    memset(&tag, 0, sizeof(BufferTag));
    Assert(strlen(table_name) < MAX_TABLE_NAME_LEN);
    memcpy(tag.tableName, table_name, strlen(table_name));
    tag.blockNum = blockNum;

    /* Lookup if tag exists in buffer table. */
    buffer = LookupBufferTable(&tag);
    if (buffer >= 0) {
        desc = GetBufferDesc(buffer);
        /* Maybe the buffer desc has unpinned and reused, 
         * neccessary to check the tag if still.*/
        if (BufferTagEquals(&tag, &desc->tag)) {
            PinBuffer(desc);
            return desc->buffer;
        }
        panic("Should not reach there.");
    }
  
    /* Missing in the entry buffer, then load new buffer desc. */
    desc = LoadNewBufferDesc(&tag);

    return desc->buffer;
}

/* Get Buffer page. */
inline void *GetBufferPage(Buffer buffer) {
    return GetBufferBlock(buffer);
}

/* Get Buffer page copy. */
inline void *GetBufferPageCopy(Buffer buffer) {
    void *block = GetBufferBlock(buffer);
    return copy_block(block, PAGE_SIZE);
}

/* Make Buffer dirty. */
inline void MakeBufferDirty(Buffer buffer) {
    void *page = GetBufferPage(buffer);
    set_node_state(page, DIRTY_STATE);
}

/* Release Buffer.
 * -----------------
 * Release Buffer after using. 
 * And this function must be called aftert ReadBuffer. */
void ReleaseBuffer(Buffer buffer) {
    ReleaseBufferInner(buffer);
}

/* Release Buffer Inner.
 * Release Buffer after using. 
 * And the function is called aftert ReadBuffer. 
 * */
void ReleaseBufferInner(Buffer buffer) {
    Assert(buffer < BUFFER_SLOT_NUM);
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    UnpinBuffer(desc);
}


/* Lock Buffer. 
 * Try to acquire the exclusive content lock in BufferDesc. 
 * */
void LockBuffer(Buffer buffer, RWLockMode mode) {
    Assert(buffer < BUFFER_SLOT_NUM);
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    /* Avoid repeatly lock. */
    AcquireRWlock(&desc->lock, mode);
}

/* Upgrade Lock Buffer. */
void UpgradeLockBuffer(Buffer buffer) {
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    UpgradeRWlock(&desc->lock);
}

/* Downgrade Lock Buffer. */
void DowngradeLockBuffer(Buffer buffer) {
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    DowngradeRWlock(&desc->lock);
}

/* Unlock Buffer
 * Unlock the exclusive content lock in BufferDesc. 
 * */
void UnlockBuffer(Buffer buffer) {
    Assert(buffer < BUFFER_SLOT_NUM);
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    ReleaseRWlock(&desc->lock);
}

/* Get Lock Mode. */
RWLockMode GetLockModeBuffer(Buffer buffer) {
    BufferDesc *desc = GetBufferDesc(buffer);
    Assert(desc != NULL);
    return desc->lock.content_lock;
}

