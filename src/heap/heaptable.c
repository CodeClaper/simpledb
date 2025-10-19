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
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "meta.h"
#include "bufmgr.h"
#include "ltree.h"
#include "select.h"

/* Heap table header length */
#define HEAP_TABLE_HEADER_LEN (NODE_STATE_SIZE + CELL_NUM_SIZE + REFER_SIZE)

/* Get Root refer. */
static inline Refer *GetRootRefer(void *root_node) {
    return (Refer *) (root_node + NODE_STATE_SIZE + CELL_NUM_SIZE);
}

/* Get cell number. */
static inline uint32_t GetPageCellNum(void *page) {
    return *(uint32_t *)(page + NODE_STATE_SIZE);
}

/* Set cell number. */
static inline void SetPageCellNum(void *page, uint32_t cell_num) {
     *(uint32_t *)(page + NODE_STATE_SIZE) = cell_num;
}

/* Get the cell data. */
static void *GetPageCellData(void *page, uint32_t cell_len, int index) {
    return (page + HEAP_TABLE_HEADER_LEN) + cell_len * index;
}

 /* If overflow page size. */
static inline bool OverflowPage(Refer *refer, uint32_t row_len) {
    return (HEAP_TABLE_HEADER_LEN + (refer->cell_num + 1) * row_len) > PAGE_SIZE;
}

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
    memcpy(GetRootRefer(rblock), rRefer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, rblock, PAGE_SIZE);
    if (w_size == -1) {
        db_log(PANIC, "Write table meta info error and errno message: %s.", strerror(errno));
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

/* Insert intadd_new_meta_columno heap table. */
static void HeapTableInsertRowInner(Oid oid, Refer *rootRefer, Row *row) {
    Table *table;
    uint32_t cell_num;
    Buffer buffer;
    void *block;

    table = open_table_inner(oid);

    /* Logically, will not overflow page size. */
    AssertFalse(OverflowPage(rootRefer, table->heap_value_len));
    buffer = ReadBuffer(rootRefer->oid, rootRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = GetPageCellNum(block);

    void *data = serialize_row_data(row, table); 
    void *destintion = GetPageCellData(block, table->heap_value_len, rootRefer->cell_num);
    /* Assign row ceontent value. */
    memcpy(destintion, data, table->heap_value_len);
    rootRefer->cell_num++;
    /* Increase cell num. */
    SetPageCellNum(block, ++cell_num);

    /* If overflow, move to next page. */
    if (OverflowPage(rootRefer, table->heap_value_len)) {
        rootRefer->page_num++;
        rootRefer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert heap table. */
Refer *HeapTableInsertRow(Oid oid, Row *row) {
    Table *table;
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer, *currentRefer;

    table = open_table_inner(oid);
    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    
    rootRefer = GetRootRefer(root);
    currentRefer = instance(Refer);
    memcpy(currentRefer, rootRefer, sizeof(Refer));
    
    /* Insert into heap table. */
    HeapTableInsertRowInner(oid, rootRefer, row);

    MakeBufferDirty(rootBuffer);
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
    
    return currentRefer;
}

/* Look up tuple from heap table. */
void *HeapTableLookupTuple(Table *table, Refer *refer) {
    Buffer buffer;
    void *block;
    
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);

    /* Deserialize row. */
    void *destintion = GetPageCellData(block, table->heap_value_len, refer->cell_num);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return destintion;
}

/* Look up tuple from heap table. */
void *HeapTableLookupTupleLoop(Table *table, Refer *refer) {
    Buffer buffer, rootBuffer;
    uint32_t cell_num;
    Refer *rootRefer;
    void *block, *root, *tuple;

    rootBuffer = ReadBuffer(refer->oid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_READERS);
    root = GetBufferBlock(rootBuffer);
    rootRefer = GetRootRefer(root);
    Assert(rootRefer->oid == refer->oid);

    /* Loop end here. */
    if (refer->page_num > rootRefer->page_num || 
        (refer->page_num == rootRefer->page_num && refer->cell_num >= rootRefer->cell_num)
    ) return NULL;
    
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferBlock(buffer);
    cell_num = GetPageCellNum(block);

    /* Deserialize row. */
    tuple = GetPageCellData(block, table->heap_value_len, refer->cell_num);
    
    /* Loop to next refer. */
    refer->cell_num++;
    if (refer->cell_num >= cell_num) refer->page_num++;

    /* Unlock and release buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);

    return tuple;
}

/* Loop up row from heap table. */
Row *HeapTableLookupRow(Table *table, Refer *refer) {
    void *tuple = HeapTableLookupTuple(table, refer);
    return GenerateRow(tuple, table->meta_table);
}

/* Update the row in heap table. */
void HeapTableUpdateRow(Table *table, Refer *refer, Row *row) {
    Buffer buffer;
    void *block;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    
    /* Update heap table row centent. */
    void *data = serialize_row_data(row, table);
    void *destintion = GetPageCellData(block, table->heap_value_len, refer->cell_num);
    memcpy(destintion, data, table->heap_value_len);
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
    destintion = GetPageCellData(block, table->heap_value_len, refer->cell_num);
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
    destintion = GetPageCellData(block, table->heap_value_len, refer->cell_num);
    *(Xid *)(destintion + table->heap_value_len - sizeof(Xid)) = expiredXid;
    MakeBufferDirty(buffer);

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


/* If overflow page after appending new column. */
static inline bool OverflowPageAfterAppendColumn(uint32_t cell_num, uint32_t cell_len, MetaColumn *newColumn) {
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
            assign_row_value(destintion + offset, newColumn->default_value, newColumn);
            break;
        }
        case DEFAULT_VALUE_NONE:
        case DEFAULT_VALUE_NULL:
            assign_row_value(destintion + offset, NULL, newColumn);
            break;
    }
    
    return destintion;
}

/* New cell postion after append column. */
static void *NewCellPostionAferAppendColumn(void *block, MetaColumn *newColumn, uint32_t cell_len, uint32_t index) {
    cell_len += newColumn->column_length;
    return GetPageCellData(block, cell_len, index);
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
    return GetPageCellData(block, cell_len, index);
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
        void *destintion = GetPageCellData(block, table->heap_value_len, i); 
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
static Refer *HeapTableSplitReInsertRow(Refer *rootRefer, Table *table, void *data) {
    Refer *refer;
    Buffer buffer;
    void *block;
    uint32_t cell_num;

    buffer = ReadBuffer(rootRefer->oid, rootRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = GetPageCellNum(block);
    refer = new_refer(rootRefer->oid, rootRefer->page_num, rootRefer->cell_num);
    
    void *destintion = GetPageCellData(block, table->heap_value_len, rootRefer->cell_num);
    memcpy(destintion, data, table->heap_value_len);
    rootRefer->cell_num++;
    /* Increase cell num. */
    SetPageCellNum(block, ++cell_num);

    /* If overflow, move to next page. */
    if (OverflowPage(rootRefer, table->heap_value_len)) {
        rootRefer->page_num++;
        rootRefer->cell_num = HEAP_TABLE_FIRST_CELL_NUM;
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return refer;
}

/* Heap table split and append new column. 
 * --------------------------------------
 * The split strategy:
 * Evenly split the page cells into two part.
 * The left part keep still the old page, and
 * the right part reinsert as new row.
 * */
static void HeapTableSplitAppendColumn(Refer *rootRefer, Table *table, MetaColumn *newColumn, 
                                       int pos, void *block, uint32_t cell_num) {
    int i;
    uint32_t left_num;
    left_num = cell_num / 2;

    /* Reinsert the right part cell. */
    for (i = cell_num - 1; i > left_num; i--) {
        void *destintion = GetPageCellData(block, table->heap_value_len, i);
        HeapTableSplitReInsertRow(rootRefer, table, destintion);
    }
    
    for (i = left_num; i >= 0; i--) {
        void *destintion = GetPageCellData(block, table->heap_value_len, i);
        memmove(
            NewCellPostionAferAppendColumn(block, newColumn, table->heap_value_len, i), 
            NewCellAfterAppendColumn(destintion, table, newColumn, pos, table->heap_value_len), 
            table->heap_value_len + newColumn->column_length
        );
    }

    /* Reset left cell num. */
    SetPageCellNum(block, left_num + 1);
}

/* Heap table append column looping each page. */
static void HeapTableAppendColumnLoop(Refer *rootRefer, Table *table, MetaColumn *newColumn, 
                                      int pos, Oid oid, int pageNum) {
    void *block;
    Buffer buffer;
    uint32_t cell_num;

    buffer = ReadBuffer(oid, pageNum);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = GetPageCellNum(block);
    
    if (OverflowPageAfterAppendColumn(cell_num, table->heap_value_len, newColumn)) 
        HeapTableSplitAppendColumn(rootRefer, table, newColumn, pos, block, cell_num);
    else 
        HeapTableAppendColumnNormal(table, newColumn, pos, block, cell_num);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Heap table append new column. */
void HeapTableAppendColumn(Table *table, MetaColumn *newColumn, int pos) {
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer;

    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    rootRefer = GetRootRefer(root);
    
    /* Handle each page. */
    for (int i = 0; i <= rootRefer->page_num; i++) {
        HeapTableAppendColumnLoop(rootRefer, table, newColumn, pos, rootRefer->oid, i);
    }

    /* Maybe root refer has changed. */
    MakeBufferDirty(rootBuffer);
    
    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
}

/* Heap table drop column looping each page. */
static void HeapTableDropColumnLoop(Table *table, MetaColumn *oldColumn, 
                                    int pos, Oid oid, int pageNum) {
    void *block;
    Buffer buffer;
    uint32_t cell_num;

    buffer = ReadBuffer(oid, pageNum);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    cell_num = GetPageCellNum(block);
    
    for (int i = 0; i < cell_num; i++) {
        void *destintion = GetPageCellData(block, table->heap_value_len, i);
        memcpy(
            NewCellPostionAferDropColumn(block, oldColumn, table->heap_value_len, i), 
            NewCellAfterDropColumn(destintion, table, oldColumn, pos, table->heap_value_len), 
            table->heap_value_len - oldColumn->column_length);
    }
    MakeBufferDirty(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Heap table drop column. */
void HeapTableDropColumn(Table *table, MetaColumn *oldColumn, int pos) {
    Buffer rootBuffer;
    void *root;
    Refer *rootRefer;

    rootBuffer = ReadBuffer(table->hoid, HEAP_TABLE_ROOT_PAGE);
    LockBuffer(rootBuffer, RW_WRITER);
    root = GetBufferBlock(rootBuffer);
    rootRefer = GetRootRefer(root);
    
    /* Handle each page. */
    for (int i = 0; i <= rootRefer->page_num; i++) {
        HeapTableDropColumnLoop(table, oldColumn, pos, rootRefer->oid, i);
    }

    UnlockBuffer(rootBuffer);
    ReleaseBuffer(rootBuffer);
}
