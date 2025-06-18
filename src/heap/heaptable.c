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

static Table *GetTableByHeapOid(Oid hoid) {
    Object obj = OidFindObject(hoid);
    Assert(obj.reltype == OHEAP_TABLE);
    return open_table(obj.relname);
}

/* Insert into heap table. */
static void HeapTableInsertRowInner(Refer *refer, Table *table, Row *row) {
    uint32_t row_len;
    Buffer buffer;
    void *block;

    row_len = calc_table_row_length(table);
    /* Logically, will not overflow page size. */
    AssertFalse(OverflowPage(refer, row_len));
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);

    void *destintion = serialize_row_data(row, table); 
    memcpy(block + refer->cell_num * row_len, destintion, row_len);
    refer->cell_num++;

    /* If overflow, move to next page. */
    if (OverflowPage(refer, row_len)) {
        refer->page_num++;
        refer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert row data to heap table. */
Refer *HeapTableInsertRow(Table *table, Row *row) {
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer, *currentRefer;

    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    
    rootRefer = GetRootRefer(root);
    currentRefer = instance(Refer);
    memcpy(currentRefer, rootRefer, sizeof(Refer));
    
    /* Insert into heap table. */
    HeapTableInsertRowInner(rootRefer, table, row);

    MakeBufferDirty(rootBuffer);
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
    
    return currentRefer;
}


/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer) {
    Buffer buffer;
    void *block;
    uint32_t row_len;
    Row *row;
    
    row_len = calc_table_row_length(table);
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);

    /* Deserialize row. */
    row = generate_row(block + refer->cell_num * row_len, table->meta_table);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return row;
}
