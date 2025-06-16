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

/* Create the heap table. */
bool CreateHeapTable(char *tableName) {
    Object entity;
    char heap_table_file[MAX_TABLE_NAME_LEN + 100];
    int descr;
    void *rblock;
    Refer *rRefer;
    Size w_size;
    
    entity = GenerateObject(tableName, OHEAP_TABLE);
    memset(heap_table_file, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(heap_table_file, "%s%ld", conf->data_dir, entity.oid);

    /* Avoid repeatly create. */
    if (table_file_exist(heap_table_file))
        return true;

    descr = open(heap_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(PANIC, "Open database file '%s' fail.", heap_table_file);
        return false;
    }
    
    rblock = dalloc(PAGE_SIZE);
    rRefer = new_refer(entity.oid, HEAP_TABLE_ROOT_PAGE, HEAP_TABLE_FIRST_CELL_NUM);
    memcpy(rblock + NODE_STATE_SIZE, rRefer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, rblock, PAGE_SIZE);
    if (w_size == -1) {
        db_log(PANIC, "Write table meta info error and errno %d.", errno);
        return false;
    } 

    /* Save the String table Object. */
    SaveObject(entity);
    
    /* Free memory. */
    dfree(rblock);
    dfree(rRefer);

    /* Close desription. */
    close(descr);

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
static void HeapTableInsertRowInner(Refer *refer, Row *row) {
    Table *table;
    uint32_t row_len;
    Buffer buffer;
    void *block;

    table = open_table_inner(refer->oid);
    if (table == NULL) {
        db_log(ERROR, "Try to open table fail");
        return;
    }
    
    row_len = calc_table_row_length(table);

    /* Logically, will not overflow page size. */
    AssertFalse(OverflowPage(refer, row_len));

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);

    memcpy(block + refer->cell_num * row_len, serialize_row_data(row, table), row_len);
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
Refer *HeapTableInsertRow(Oid oid, Row *row) {
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer, *currentRefer;

    rootBuffer = ReadBuffer(oid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    
    rootRefer = GetRootRefer(root);
    currentRefer = instance(Refer);
    memcpy(currentRefer, rootRefer, sizeof(Refer));

    HeapTableInsertRowInner(rootRefer, row);

    MakeBufferDirty(rootBuffer);
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
    
    return currentRefer;
}
