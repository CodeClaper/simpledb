#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "heaptable.h"
#include "bufpool.h"
#include "sys.h"
#include "systable.h"
#include "defs.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "meta.h"
#include "bufmgr.h"
#include "ltree.h"
#include "select.h"

/* Create table inner. */
bool CreateHeapTableInner(Oid oid) {
    int descr;
    void *rblock;
    Refer *rRefer;
    char heap_table_file[MAX_TABLE_NAME_LEN + 100];
    Size w_size;

    memset(heap_table_file, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(heap_table_file, "%s%ld", conf->data_dir, oid);

    /* Avoid repeatly create. */
    if (table_file_exist(heap_table_file))
        return true;

    descr = open(heap_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(PANIC, "Open database file '%s' fail.", heap_table_file);
        return false;
    }
    
    rblock = dalloc(PAGE_SIZE);
    rRefer = new_refer(oid, HEAP_TABLE_ROOT_PAGE, HEAP_TABLE_FIRST_CELL_NUM);
    memcpy(rblock + NODE_STATE_SIZE, rRefer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, rblock, PAGE_SIZE);
    if (w_size == -1) {
        db_log(PANIC, "Write table meta info error and errno %d.", errno);
        return false;
    } 

    /* Free memory. */
    dfree(rblock);
    dfree(rRefer);

    /* Close desription. */
    close(descr);

    return true;
}

/* Create the heap table. */
bool CreateHeapTable(char *tableName) {

    Object entity = GenerateObject(tableName, OHEAP_TABLE);
    
    /* Create the heap table. */
    CreateHeapTableInner(entity.oid);

    /* Save the String table Object. */
    SaveObject(entity);

    return true;
}

/* Get Root refer. */
static inline Refer *GetRootRefer(void *root_node) {
    return (Refer *) (root_node + NODE_STATE_SIZE);
}

/* If overflow page size. */
static inline bool OverflowPage(Refer *refer, uint32_t row_len) {
    return (refer->cell_num + 1) * row_len > PAGE_SIZE;
}

/* Insert into heap table. */
static void HeapTableInsertRowInner(Refer *refer, Cursor *cursor, Row *row) {
    Table *table;
    uint32_t row_len, cell_len;
    Buffer buffer;
    Refer *iRefer;
    void *block;

    table = cursor->table;
    row_len = calc_table_row_length(table);
    cell_len = REFER_SIZE + row_len;
    /* Logically, will not overflow page size. */
    AssertFalse(OverflowPage(refer, cell_len));
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);

    iRefer = convert_refer(cursor);
    void *destintion = serialize_row_data(row, table); 
    /* Assign index refer value. */
    memcpy(block + refer->cell_num * cell_len, iRefer, REFER_SIZE);
    /* Assign row ceontent value. */
    memcpy(block + refer->cell_num * cell_len + REFER_SIZE, destintion, row_len);
    refer->cell_num++;

    /* If overflow, move to next page. */
    if (OverflowPage(refer, cell_len)) {
        refer->page_num++;
        refer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert row data to heap table. */
Refer *HeapTableInsertRow(Cursor *cursor, Row *row) {
    Table *table;
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer, *currentRefer;

    table = cursor->table;
    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    
    rootRefer = GetRootRefer(root);
    currentRefer = instance(Refer);
    memcpy(currentRefer, rootRefer, sizeof(Refer));
    
    /* Insert into heap table. */
    HeapTableInsertRowInner(rootRefer, cursor, row);

    MakeBufferDirty(rootBuffer);
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
    
    return currentRefer;
}


/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer) {
    Buffer buffer;
    void *block;
    uint32_t row_len, cell_len;
    Row *row;
    
    row_len = calc_table_row_length(table);
    cell_len = row_len + REFER_SIZE;
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);

    /* Deserialize row. */
    row = generate_row(block + refer->cell_num * cell_len + REFER_SIZE, table->meta_table);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return row;
}

/* Update the row in heap table. */
void HeapTableUpdateRow(Table *table, Refer *refer, Row *row) {
    Buffer buffer;
    void *block;
    uint32_t row_len, cell_len;

    row_len = calc_table_row_length(table);
    cell_len = row_len + REFER_SIZE;
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    
    /* Update heap table row centent. */
    void *destintion = serialize_row_data(row, table);
    memcpy(block + refer->cell_num * cell_len + REFER_SIZE, destintion, row_len);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Drop the heap table. */
bool DropHeapTable(char *tableName) {
    Oid oid;
    char *heap_table_file;

    oid = TableNameFindHeapOid(tableName);
    AssertFalse(ZERO_OID(oid));
    heap_table_file = table_file_path(oid);

    if (!check_table_exist_direct(oid)) {
        db_log(ERROR, "Heap table file '%s' not exists, error : %s", 
               heap_table_file, strerror(errno));
        return false;
    }

    /* Delete physically. */
    if (remove(heap_table_file) == 0 && RemoveObject(oid))
        return true;

    /* Not reach here logically. */
    db_log(ERROR, 
           "Try to drop heap table '%s' fail, error : %s", 
           tableName, strerror(errno));

    return false;
}

void HeapTableAppendColumn(Table *table, MetaColumn *newColumn, int pos) {
    
}
