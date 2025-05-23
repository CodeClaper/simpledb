#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "buftable.h"
#include "spinlock.h"
#include "mmgr.h"
#include "hash.h"

/* BTable is a hash table. */
static BufferTableEntrySlot *BTable;

/* Geneate new BufferTableEntry. */
static BufferTableEntry *NewBufferTableEntry(BufferTag *tag, Buffer buffer) {
    switch_shared();
    BufferTableEntry *entry = instance(BufferTableEntry);
    entry->buffer = buffer;
    entry->tag.oid = tag->oid;
    entry->tag.blockNum = tag->blockNum;
    entry->next = NULL;
    switch_local();
    return entry;
}

/* Get Buffer Table slot. */
inline BufferTableEntrySlot *GetBufferTableSlot(BufferTag *tag) {
    Hash hash = OidHash((tag->oid + tag->blockNum), BUFFER_SLOT_NUM);
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
        header->lock = instance(s_lock);
        init_spin_lock(header->lock);
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

    /* Wait for lock relase.
     * Note: this mechanism maybe not safe when insert after read.
     * */
    wait_for_spin_lock(slot->lock);

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
 * -------------------------
 * BufferTableEntry link the BufferTag and Buffer. 
 * Note: This <InsertBufferTableEntry> need acquire the rwlock in exclusive mode. 
 * But not acquire itself, and by the caller.
 * */
void InsertBufferTableEntry(BufferTag *tag, Buffer buffer) {
    BufferTableEntrySlot *slot;
    BufferTableEntry *entry;

    slot = GetBufferTableSlot(tag);
    entry = slot->next;
    Assert(LOCKED(*slot->lock));

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
void RemoveTableBuffer(Oid oid) {
    switch_shared();
    for (Index i = 0; i < BUFFER_SLOT_NUM; i++) {
        BufferTableEntrySlot *slot; 
        BufferTableEntry *pres, *current;

        slot = GetBufferTableSlotByIndex(i);
        current = slot->next;
        for (current = slot->next, pres = current; current != NULL; pres = current, current = current->next) {
            BufferTag tag = current->tag;
            if (tag.oid == oid) {
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

static void PrintBufTable(int k) {
    for (Index i = (0 * k * 1000); i < (k + 1) *1000; i++) {
        int deep;
        BufferTableEntrySlot *slot ;
        BufferTableEntry *current;
        
        slot = GetBufferTableSlotByIndex(i);
        for (current = slot->next, deep = 0; 
                current != NULL; 
                    current = current->next, deep++);
        printf("%d\t", deep);
    }
}
