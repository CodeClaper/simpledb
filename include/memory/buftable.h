#include "bufmgr.h"
#include "bufpool.h"
#include "rwlock.h"

typedef struct BufferTableEntrySlot {
    s_lock lock;
    struct BufferTableEntry *next;
} BufferTableEntrySlot;

typedef struct BufferTableEntry {
    BufferTag tag;
    Buffer buffer;
    struct BufferTableEntry *next;
} BufferTableEntry;

/* Create the buffer table.*/
void CreateBufferTable();

/* Get Buffer Table slot. */
BufferTableEntrySlot *GetBufferTableSlot(BufferTag *tag);

/* Return Buffer of BufferDesc, 
 * and -1 if not found.*/
Buffer LookupBufferTable(BufferTag *tag);

/* Save new BufferTableEntry 
 * which link the BufferTag and Buffer. */
void InsertBufferTableEntry(BufferTag *tag, Buffer buffer);

/* Delete the BufferTableEntry by tag.*/
void DeleteBufferTableEntry(BufferTag *tag);

/* Remove all the table-relative buffer entry. */
void RemoveTableBuffer(char *table_name);
