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
#include "row.h"
#include "tuple.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "meta.h"
#include "bufmgr.h"
#include "buftable.h"
#include "fdesc.h"
#include "instance.h"
#include "trans.h"

/* Heap table header length */
#define HEAP_TABLE_HEADER_LEN (NODE_STATE_SIZE + CELL_NUM_SIZE + REFER_SIZE)

/* Get Root refer. */
static inline Refer *HeapTableGetRootRefer(void *root_node) {
    return (Refer *) (root_node + NODE_STATE_SIZE + CELL_NUM_SIZE);
}

/* Get cell number. */
static inline uint32_t HeapTableGetPageCellNum(void *page) {
    return *(uint32_t *)(page + NODE_STATE_SIZE);
}

/* Set cell number. */
static inline void HeapTableSetPageCellNum(void *page, uint32_t cell_num) {
     *(uint32_t *)(page + NODE_STATE_SIZE) = cell_num;
}

/* Get the cell data. */
static void *HeapTableGetPageCellData(void *page, uint32_t cell_len, int index) {
    return (page + HEAP_TABLE_HEADER_LEN) + cell_len * index;
}

 /* If overflow page size. */
static inline bool HeapTableOverflowForInsert(Refer *refer, uint32_t row_len) {
    return (HEAP_TABLE_HEADER_LEN + (refer->cell_num + 1) * row_len) > PAGE_SIZE;
}

/* Move next page. */
static void HeapTableMoveNextPage(Refer *rootRefer) {
    Buffer buffer;
    void *block;
    
    rootRefer->page_num++;
    rootRefer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
    buffer = ReadBuffer(rootRefer->oid, rootRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferPage(buffer);
    
    HeapTableSetPageCellNum(block, 0);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Create table inner. */
bool CreateHeapTableInner(Oid hoid) {
    int descr;
    void *rblock;
    Refer *rRefer;
    char heap_table_file[MAX_TABLE_NAME_LEN + 100];
    Size w_size;

    memset(heap_table_file, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(heap_table_file, "%s%ld", conf->data_dir, hoid);

    /* Avoid repeatly create. */
    if (table_file_exist(heap_table_file)) {
        logger(PANIC, "Heap table file %s alreay exists.", heap_table_file);
        return true;
    }

    descr = open(heap_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        logger(PANIC, "Open database file '%s' fail.", heap_table_file);
        return false;
    }
    
    rblock = dalloc(PAGE_SIZE);
    rRefer = new_refer(hoid, HEAP_TABLE_ROOT_PAGE, HEAP_TABLE_FIRST_CELL_NUM);
    memcpy(HeapTableGetRootRefer(rblock), rRefer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, rblock, PAGE_SIZE);
    if (w_size == -1) {
        logger(PANIC, "Write table meta info error and errno message: %s.", strerror(errno));
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
bool CreateHeapTable(Oid oid, Oid toid, char *tableName) {
    Object entity = GenerateObjectInner(oid, toid, tableName, OHEAP_TABLE);
    return CreateHeapTableInner(entity.oid) && SaveObject(entity);
}

/* Insert intadd_new_meta_columno heap table. */
static void HeapTableInsertTupleInner(Oid oid, Refer *rootRefer, void *tuple) {
    Table *table;
    uint32_t cell_num;
    Buffer buffer;
    void *block, *destintion;

    table = open_table_inner(oid);

    /* Logically, will not overflow page size. */
    AssertFalse(HeapTableOverflowForInsert(rootRefer, table->heap_value_len));
    buffer = ReadBuffer(rootRefer->oid, rootRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = HeapTableGetPageCellNum(block);

    /* Seriable data and set. */
    destintion = HeapTableGetPageCellData(block, table->heap_value_len, rootRefer->cell_num);
    memcpy(destintion, tuple, table->heap_value_len);
    rootRefer->cell_num++;

    /* Increase cell num. */
    HeapTableSetPageCellNum(block, ++cell_num);

    /* If overflow, move to next page. */
    if (HeapTableOverflowForInsert(rootRefer, table->heap_value_len)) 
        HeapTableMoveNextPage(rootRefer);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert heap table. */
Refer *HeapTableInsertTuple(Oid oid, void *tuple) {
    Table *table;
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer, *currentRefer;

    table = open_table_inner(oid);
    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    
    rootRefer = HeapTableGetRootRefer(root);
    Assert(rootRefer->oid == table->hoid);
    currentRefer = instance(Refer);
    memcpy(currentRefer, rootRefer, sizeof(Refer));
    
    /* Insert into heap table. */
    HeapTableInsertTupleInner(oid, rootRefer, tuple);

    MakeBufferDirty(rootBuffer);
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
    
    return currentRefer;
}

/* Look up tuple from heap table. */
void *HeapTableLookupTuple(Oid oid, Refer *refer) {
    Oid hoid;
    Table *table;
    Buffer buffer, rootBuffer;
    Refer *rootRefer;
    void *block, *root, *tuple = NULL;

    table = open_table_inner(oid);
    hoid = table->hoid;
    rootBuffer = ReadBuffer(hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_READERS);
    root = GetBufferBlock(rootBuffer);
    rootRefer = HeapTableGetRootRefer(root);
    Assert(rootRefer->oid == hoid);

    /* If overflow, just return NULL. */
    if (refer->page_num > rootRefer->page_num || 
        (refer->page_num == rootRefer->page_num && refer->cell_num >= rootRefer->cell_num)
    ) goto direct_exist;
    
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);

    /* Get tuple in cell. */
    tuple = HeapTableGetPageCellData(block, table->heap_value_len, refer->cell_num);
    
    /* Unlock and release buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

direct_exist:
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);

    return tuple;
}

/* Heap table iterator. */
void HeapTableIteratorRefer(Refer *refer) {
    Buffer buffer;
    void *block;
    uint32_t cell_num;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);
    cell_num = HeapTableGetPageCellNum(block);

    refer->cell_num++;
    if (refer->cell_num >= cell_num) { 
        refer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
        refer->page_num++;
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Oid oid, Refer *refer) {
    Table *table = open_table_inner(oid);
    void *tuple = HeapTableLookupTuple(oid, refer);
    return GenerateRow(tuple, table->meta_table);
}

/* Update the row in heap table. */
void HeapTableUpdateTuple(Oid oid, Refer *refer, void *tuple) {
    Table *table;
    Buffer buffer;
    void *block, *destintion;

    table = open_table_inner(oid);
    Assert(table->hoid == refer->oid);
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    
    /* Update heap table row centent. */
    destintion = HeapTableGetPageCellData(block, table->heap_value_len, refer->cell_num);
    memcpy(destintion, tuple, table->heap_value_len);
    MakeBufferDirty(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Update the heap table row createdXid. */
void HeapTableUpdateRowCreatedXid(Table *table, Refer *refer, Xid createdXid) {
    Buffer buffer;
    void *block, *destintion;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    
    /* Update heap table row createdXid. */
    destintion = HeapTableGetPageCellData(block, table->heap_value_len, refer->cell_num);
    *(Xid *)(destintion + table->heap_value_len - sizeof(Xid) - LEAF_NODE_CELL_NULL_FLAG_SIZE - sizeof(Xid)) = createdXid;
    MakeBufferDirty(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Update the heap table row createdXid. */
void HeapTableUpdateRowExpiredXid(Table *table, Refer *refer, Xid expiredXid) {
    Buffer buffer;
    void *block, *destintion;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    
    /* Update heap table row createdXid. */
    destintion = HeapTableGetPageCellData(block, table->heap_value_len, refer->cell_num);
    *(Xid *)(destintion + table->heap_value_len - sizeof(Xid)) = expiredXid;
    MakeBufferDirty(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Drop heap table from */
static bool DropHeapTableFromDisk(Oid hoid) {
    char *heap_table_file = table_file_path(hoid);
    if (!check_table_exist_direct(hoid)) {
        logger(ERROR, "Heap table file '%s' not exists, error : %s", 
               heap_table_file, strerror(errno));
        return false;
    }
    
    /* It will do:
     * (1) Remove table buffer. 
     * (2) Remove file from disk. 
     * */
    if ( 
        RemoveTableBuffer(hoid) &&
        remove(heap_table_file) == 0
    ) {
        /* Unregister fdesc. */
        unregister_fdesc(hoid);
        return true;
    } 

    return false;
}

/* Drop the heap table. */
bool DropHeapTable(Oid hoid) {
    /* Todo list:
     * (1) Regster the DropHeapTableFromDisk to trans commit event. 
     * (2) Remove object systable. */
    if (
        RemoveObject(hoid) &&
        RegisterCommitEvent(DropHeapTableFromDisk, hoid)  
    ) return true;

    /* Not reach here logically. */
    logger(ERROR, "Try to drop heap file '%ld' fail, error : %s", 
           hoid, strerror(errno));

    return false;
}


/* If overflow page after appending new column. */
static inline bool HeapTableOverflowForAppend(uint32_t cell_num, uint32_t cell_len, MetaColumn *newColumn) {
    return HEAP_TABLE_HEADER_LEN + cell_num * (cell_len + newColumn->column_length) > PAGE_SIZE;
}

/* Calcualte the offset where new column appending at. */
static uint32_t CalcOffsetByPos(MetaTable *meta_table, int pos) {
    /*  Calcualte offset. */
    uint32_t offset = 0;
    for (int i = 0; i < pos; i++) {
        ListCell *lc = list_nth_cell(meta_table->meta_columns, i);
        MetaColumn *current = (MetaColumn *)lfirst(lc);
        offset += current->column_length;
    }
    return offset;
}

/* New cell data after append column. */
static void *NewCellAfterAppendColumn(void *destintion, Table *table, MetaColumn *newColumn, int pos, uint32_t cell_len) {
    uint32_t offset = CalcOffsetByPos(table->meta_table, pos);
    Assert(cell_len > offset);
    
    /* Move after new column memcpy. */
    memmove(destintion + offset + newColumn->column_length, 
            destintion + offset, cell_len - offset);

    /* Assign new column default value. */
    switch (newColumn->default_value_type) {
        case DEFAULT_VALUE: {
            /* Maybe default value is null, when refer value not found any match row. */
            MetaColumnAssignValueToDestination(destintion + offset, newColumn->default_value, newColumn);
            break;
        }
        case DEFAULT_VALUE_NONE:
        case DEFAULT_VALUE_NULL:
            MetaColumnAssignValueToDestination(destintion + offset, NULL, newColumn);
            break;
    }
    
    return destintion;
}

/* New cell postion after append column. */
static void *NewCellPostionAferAppendColumn(void *block, MetaColumn *newColumn, uint32_t cell_len, uint32_t index) {
    cell_len += newColumn->column_length;
    return HeapTableGetPageCellData(block, cell_len, index);
}

/* New cell data after drop column. */
static void *NewCellAfterDropColumn(void *destintion, Table *table, MetaColumn *oldColumn, int pos, uint32_t cell_len) {
    uint32_t offset = CalcOffsetByPos(table->meta_table, pos);
    Assert(cell_len > offset);
    
    /* Move after new column memcpy. */
    memcpy(destintion + offset, 
           destintion + offset + oldColumn->column_length, 
           cell_len - offset - oldColumn->column_length);

    return destintion;
}

/* New cell postion after drop column. */
static void *NewCellPostionAferDropColumn(void *block, MetaColumn *oldColumn, uint32_t cell_len, uint32_t index) {
    cell_len -= oldColumn->column_length;
    return HeapTableGetPageCellData(block, cell_len, index);
}

/* Heap table append column for normal. 
 * ----------------------------
 * Normal means just move space for new column.
 * No need to think about cross page.
 * No need to update the refer.
 * */
static void HeapTableAppendColumnNormal(Table *table, MetaColumn *newColumn, int pos, void *block, uint32_t cell_num) {
    int i;
    for (i = cell_num - 1; i >= 0; i--) {
        void *destintion = HeapTableGetPageCellData(block, table->heap_value_len, i); 
        memmove(
            NewCellPostionAferAppendColumn(block, newColumn, table->heap_value_len, i), 
            NewCellAfterAppendColumn(destintion, table, newColumn, pos, table->heap_value_len), 
            table->heap_value_len + newColumn->column_length
        );
    }
}

/* Heap table re-insert row when split.
 * -----------------------------------
 * Return refer of re-insert-row postion, which freed by caller.
 * */
static void HeapTableSplitReInsert(Refer *rootRefer, Table *table, void *tuple) {
    Buffer buffer;
    void *block;
    uint32_t cell_num;

    /* Logically, will not overflow page size. */
    AssertFalse(HeapTableOverflowForInsert(rootRefer, table->heap_value_len));
    buffer = ReadBuffer(rootRefer->oid, rootRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = HeapTableGetPageCellNum(block);
    
    void *destintion = HeapTableGetPageCellData(block, table->heap_value_len, rootRefer->cell_num);
    memcpy(destintion, tuple, table->heap_value_len);
    rootRefer->cell_num++;
    /* Increase cell num. */
    HeapTableSetPageCellNum(block, ++cell_num);

    /* If overflow, move to next page. */
    if (HeapTableOverflowForInsert(rootRefer, table->heap_value_len))
        HeapTableMoveNextPage(rootRefer);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Heap table split and append new column. 
 * --------------------------------------
 * The split strategy:
 * Evenly split the page cells into two part.
 * The left part keep still the old page, and
 * the right part reinsert as new row.
 * */
static void HeapTableSplitAppendColumn(Refer *rootRefer, Table *table, MetaColumn *newColumn, 
                                       int pos, void *block, uint32_t page_num, uint32_t cell_num) {
    int i;
    uint32_t left_num;
    void *tuple;
    left_num = cell_num / 2;

    /* If root refer page is overflow after appending, we need move to next page to avoid 
     * reinsert data will be covered by the following code. */
    if (rootRefer->page_num == page_num)
        HeapTableMoveNextPage(rootRefer);

    /* Reinsert the right part cell. */
    for (i = left_num + 1; i < cell_num; i++) {
        tuple = HeapTableGetPageCellData(block, table->heap_value_len, i);
        HeapTableSplitReInsert(rootRefer, table, tuple);
    }
    
    for (i = left_num; i >= 0; i--) {
        tuple = HeapTableGetPageCellData(block, table->heap_value_len, i);
        memmove(
            NewCellPostionAferAppendColumn(block, newColumn, table->heap_value_len, i), 
            NewCellAfterAppendColumn(tuple, table, newColumn, pos, table->heap_value_len), 
            table->heap_value_len + newColumn->column_length
        );
    }

    /* Reset left cell num. */
    HeapTableSetPageCellNum(block, left_num + 1);
}

/* Heap table append column looping each page. */
static void HeapTableAppendColumnForeachPage(Refer *rootRefer, Oid oid, Oid hoid,
                                             int page_num, int pos, MetaColumn *newColumn) {
    Table *table;
    void *block;
    Buffer buffer;
    uint32_t cell_num;

    table = open_table_inner(oid);
    buffer = ReadBuffer(hoid, page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = HeapTableGetPageCellNum(block);
    
    if (HeapTableOverflowForAppend(cell_num, table->heap_value_len, newColumn)) 
        HeapTableSplitAppendColumn(rootRefer, table, newColumn, pos, block, page_num, cell_num);
    else 
        HeapTableAppendColumnNormal(table, newColumn, pos, block, cell_num);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Heap table append new column. */
void HeapTableAppendColumn(Oid oid, MetaColumn *newColumn, int pos) {
    Oid hoid;
    Table *table;
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer;

    table = open_table_inner(oid);
    hoid = table->hoid;
    rootBuffer = ReadBuffer(hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    rootRefer = HeapTableGetRootRefer(root);
    
    /* Handle each page. */
    for (int i = 0; i <= rootRefer->page_num; i++) 
        HeapTableAppendColumnForeachPage(rootRefer, oid, hoid, i, pos,newColumn);

    /* Maybe root refer has changed. */
    MakeBufferDirty(rootBuffer);
    
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
}

/* Heap table drop column looping each page. */
static void HeapTableDropColumnForeachPage(Oid oid, Oid hoid, int pageNum, int pos, MetaColumn *oldColumn) {
    Table *table;
    void *block;
    Buffer buffer;
    uint32_t cell_num;

    table = open_table_inner(oid);
    buffer = ReadBuffer(hoid, pageNum);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = HeapTableGetPageCellNum(block);
    
    for (int i = 0; i < cell_num; i++) {
        void *destintion = HeapTableGetPageCellData(block, table->heap_value_len, i);
        memcpy(NewCellPostionAferDropColumn(block, oldColumn, table->heap_value_len, i), 
               NewCellAfterDropColumn(destintion, table, oldColumn, pos, table->heap_value_len), 
               table->heap_value_len - oldColumn->column_length);
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Heap table drop column. */
void HeapTableDropColumn(Oid oid, MetaColumn *oldColumn, int pos) {
    Oid hoid;
    Table *table;
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer;

    table = open_table_inner(oid);
    hoid = table->hoid;
    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    rootRefer = HeapTableGetRootRefer(root);
    
    /* Handle each page. */
    for (int i = 0; i <= rootRefer->page_num; i++) 
        HeapTableDropColumnForeachPage(oid, hoid, i, pos, oldColumn);

    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
}
