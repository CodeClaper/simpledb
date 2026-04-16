#include "bufmgr.h"
#include "rwlock.h"
#include <stdbool.h>

typedef struct BufferTableEntrySlot {
    s_lock *lock;
    struct BufferTableEntry *next;
} BufferTableEntrySlot;

typedef struct BufferTableEntry {
    BufferTag tag;
    Buffer buffer;
    struct BufferTableEntry *next;
} BufferTableEntry;

void CreateBufferTable();
BufferTableEntrySlot *GetBufferTableSlot(BufferTag *tag);
Buffer LookupBufferTable(BufferTag *tag);
Buffer LookupBufferTableWithoutLock(BufferTag *tag);
void InsertBufferTableEntry(BufferTag *tag, Buffer buffer);
void DeleteBufferTableEntry(BufferTag *tag);
bool RemoveTableBuffer(Oid oid);
