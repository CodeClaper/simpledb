#include <stdint.h>
#include <string.h>
#include "buftable.h"
#include "spinlock.h"
#include "mmgr.h"

/* BTable is a hash table. */
static BufferTableEntrySlot *BTable;

/* HashCode. */
typedef unsigned long Hash;

/* Hash the BufferTag. */
static Hash HashBufferTag(BufferTag *tag, Size size) {
    /* Use DJB2 hash alg. */
    unsigned long hash = 5381;
    hash = ((hash << 5) + hash) + tag->blockNum;
    const char *tableName = tag->tableName;
    int c;
    while ((c = *tableName++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % size;
}

/* Geneate new BufferTableEntry. */
static BufferTableEntry *NewBufferTableEntry(BufferTag *tag, Buffer buffer) {
    switch_shared();
    BufferTableEntry *entry = instance(BufferTableEntry);
    entry->buffer = buffer;
    entry->tag.blockNum = tag->blockNum;
    memcpy(entry->tag.tableName, tag->tableName, MAX_TABLE_NAME_LEN);
    entry->next = NULL;
    switch_local();
    return entry;
}

/* Get Buffer Table slot. */
inline BufferTableEntrySlot *GetBufferTableSlot(BufferTag *tag) {
    Hash hash = HashBufferTag(tag, BUFFER_SLOT_NUM);
    return (BufferTableEntrySlot *)(BTable + hash);
}


/* Get Buffer Table slot by index. */
static inline BufferTableEntrySlot *GetBufferTableSlotByIndex(Index idx) {
     return (BufferTableEntrySlot *)(BTable + idx);
}

/* Create the buffer table.*/
void CreateBufferTable() {
    switch_shared();
    BTable = dalloc(sizeof(BufferTableEntrySlot) * BUFFER_SLOT_NUM);
    for (Index i = 0; i < BUFFER_SLOT_NUM; i++) {
        BufferTableEntrySlot *header = GetBufferTableSlotByIndex(i);
        header->lock = instance(RWLockEntry);
        InitRWlock(header->lock);
    }
    switch_local();
}


/* Lookup for Buffer in entry table. 
 * -------------------------
 * Return the found buffer and -1 if not found. */
Buffer LookupBufferTable(BufferTag *tag) {
    Buffer buffer;
    BufferTableEntrySlot *slot;
    BufferTableEntry *entry;

    buffer = -1;
    slot = GetBufferTableSlot(tag);
    entry = slot->next;

    /* Acquire the rwlock in shared mode.*/
    AcquireRWlock(slot->lock, RW_READERS);

    /* Loop up the entry table. */
    while (entry != NULL) {
        if (BufferTagEquals(&entry->tag, tag)) {
            buffer = entry->buffer;
            break;
        }
        entry = entry->next;
    }

    ReleaseRWlock(slot->lock);

    return buffer;
}

/* Lookup for Buffer in entry table. 
 * -------------------------
 * Return the found buffer and -1 if not found. */
Buffer LookupBufferTableWithoutLock(BufferTag *tag) {
    Buffer buffer;
    BufferTableEntrySlot *slot;
    BufferTableEntry *entry;

    buffer = -1;
    slot = GetBufferTableSlot(tag);
    entry = slot->next;

    /* Loop up the entry table. */
    while (entry != NULL) {
        if (BufferTagEquals(&entry->tag, tag)) {
            buffer = entry->buffer;
            break;
        }
        entry = entry->next;
    }

    return buffer;
}


/* Save new BufferTableEntry 
 * ---------------
 * BufferTableEntry link the BufferTag and Buffer. 
 * Note: This <InsertBufferTableEntry> need acquire the rwlock in exclusive mode. 
 * But not acquire itself, and by the caller.
 * */
void InsertBufferTableEntry(BufferTag *tag, Buffer buffer) {
    BufferTableEntrySlot *slot;
    BufferTableEntry *entry;

    slot = GetBufferTableSlot(tag);
    entry = slot->next;
    Assert(slot->lock->mode == RW_WRITER);

    if (entry == NULL) {
        slot->next = NewBufferTableEntry(tag, buffer);
        return;
    }

    /* Find the tail. */
    while (entry->next) {
        entry = entry->next;
        AssertFalse(BufferTagEquals(tag, &entry->tag));
    }
    entry->next = NewBufferTableEntry(tag, buffer);
}

/* Delete the BufferTableEntry by tag.
 * ------------
 * Note: This <DeleteBufferTableEntry> need acquire the rwlock in exclusive mode. 
 * But not acquire itself, and by the caller.
 * */
void DeleteBufferTableEntry(BufferTag *tag) {
    BufferTableEntrySlot *slot; 
    BufferTableEntry *pres, *current;

    slot = GetBufferTableSlot(tag);
    current = slot->next;

    switch_shared();
    for (current = slot->next, pres = current; current != NULL; pres = current, current = current->next) {
        if (BufferTagEquals(&current->tag, tag)) {
            if (current == slot->next) 
                slot->next = current->next;
            else 
                pres->next = current->next;
            
            /* Necessary to free the shared memory. */
            dfree(current);
        }
    }
    switch_local();
}

/* Remove all the table-relative buffer entry. */
void RemoveTableBuffer(char *table_name) {
    switch_shared();
    for (Index i = 0; i < BUFFER_SLOT_NUM; i++) {
        BufferTableEntrySlot *slot; 
        BufferTableEntry *pres, *current;

        slot = GetBufferTableSlotByIndex(i);
        current = slot->next;
        for (current = slot->next, pres = current; current != NULL; pres = current, current = current->next) {
            BufferTag tag = current->tag;
            if (streq(tag.tableName, table_name)) {
                if (current == slot->next) 
                    slot->next = current->next;
                else 
                    pres->next = current->next;
                /* Necessary to free the shared memory. */
                dfree(current);
            }
        }
    }
    switch_local();
}
