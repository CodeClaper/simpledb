#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "arrheaptable.h"
#include "instance.h"
#include "systable.h"
#include "table.h"
#include "mmgr.h"
#include "log.h"
#include "fdesc.h"
#include "bufmgr.h"
#include "copy.h"
#include "list.h"

#define ARRAY_TABLE_ROOT_PAGE   0
#define ARRAY_TABLE_FIRST_NUM   1
#define ARRAY_TABLE_ROW_NUM     64
#define ARRAY_TABLE_ROW_SIZE    (PAGE_SIZE / ARRAY_TABLE_ROW_NUM)


/* Get the root refer. 
 * Skip the NODE_STATE_SIZE. 
 * root_node:   Root node.
 * Return:      Root refer.
 * */
static inline Refer *GetRootRefer(void *root_node) {
    return (Refer *) (root_node + NODE_STATE_SIZE);
}

/* Calculate page will be overflow. 
 * refer:   The refer of array value will be posted.
 * size:    Size the bound.
 * Return:  Will overflow or not
 * */
static bool ArrayTablePageWillOverflow(Refer *refer, Size size) {
    uint32_t useRowNum, leftRowNum;
    
    useRowNum = size / ARRAY_TABLE_ROW_SIZE;
    if (size % ARRAY_TABLE_ROW_SIZE != 0) useRowNum++;
    leftRowNum = ARRAY_TABLE_ROW_NUM - refer->cell_num;

    return leftRowNum >= useRowNum;
}

/* Create array heap table inner.
 * oid:         Array heap table oid.
 * Return:      Success or fail.
 * */
static bool CreateArrayHeapTableInner(Oid oid) {
    int descr, w_size;
    void *block;
    Refer *refer;
    char file_path[MAX_TABLE_NAME_LEN + 100];
    
    memset(file_path, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(file_path, "%s%ld", conf->data_dir, oid);

    /* Avoid repeatly create. */
    if (table_file_exist(file_path)) {
        THROW("String heap table file %s alreay exists.", file_path);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        THROW("Open database file '%s' fail.", file_path);
        return false;
    }

    /* Initialize page. */
    block = dalloc(PAGE_SIZE);
    refer = new_refer(oid, ARRAY_TABLE_ROOT_PAGE, ARRAY_TABLE_FIRST_NUM);
    memcpy(block + NODE_STATE_SIZE, refer, sizeof(Refer));

    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, block, PAGE_SIZE);
    if (w_size == -1) {
        THROW("Write table meta info error and error message: %s.", strerror(errno));
        return false;
    } 

    dfree(block);
    dfree(refer);
    close(descr);

    return true;
}

/* Create array heap table.
 * oid:         Array heap table oid.
 * tid:         Table oid.
 * table_name:  Table name.
 * Return:      Success or fail. 
 * */
bool CreateArrayHeapTable(Oid oid, Oid tid, char *table_name) {
    Object entity = GenerateObjectInner(oid, tid, table_name, OARRAY_HEAP_TABLE);
    return CreateArrayHeapTableInner(oid) && SaveObject(entity);
}

/* Query array value. */
ArrayValue *QueryArrayValue(Refer *refer, MetaColumn *meta_column) {
    Assert(meta_column->array_dim > 0);
    return NULL;
}

/* Calculate bound size. 
 * If the array is 3D array, the M x N x P is value size, and three int size is the dim size.
 * meta_column: Meta column.
 * bound:       bound.
 * */
static Size CalcArrayValueBoundSize(MetaColumn *meta_column, List *bound) {
    Size size = 1;
    ListCell *lc;
    foreach (lc, bound) {
        size = size * lfirst_int(lc);
    }
    return (sizeof(int) * bound->size) + size * meta_column->column_length;
}

/* Calculate array value bound. 
 * Note: every element in the same dim has the same size, so we just consider the first element.
 * array:       Array value.
 * meta_column: Meta column.
 * bound:       bound.
 * */
static void CalcArrayValueBound(ArrayValue *array, MetaColumn *meta_column, List *bound) {
    ArrayValue *current = array;
    Assert(meta_column->array_dim > 0);
    for (int i = 0; i < meta_column->array_dim; i++) {
        append_list_int(bound, current->list->size);
        current = lfirst(first_cell(current->list));
        AssertFalse(current == NULL);
    }
}

/* Calculate array value values. 
 * Note: every element in the same dim has the same size, so we just consider the first element.
 * array:       Array value.
 * meta_column: Meta column.
 * values:      values.
 * */
static void CalcArrayValueValues(ArrayValue *array, int idx, List *values) {
    ListCell *lc;
    foreach(lc, array->list) {
        if (idx > 0) CalcArrayValueValues(lfirst(lc), idx - 1, values);
        else append_list(values, lfirst(lc));
    }
}

/* Insert array value case cross page. */
static void InsertArrayValueCrossPage(Refer *refer, MetaColumn *meta_column, List *values, List *bound) {
    Buffer buffer;
    void *block, *dest;
    size_t offset;
    ListCell *lc1, *lc2;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    offset = 0;

    /* Assign bound meta info. */
    foreach(lc1, bound) {
        memcpy(dest + offset, lfirst(lc1), sizeof(int));
        offset += sizeof(int);
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert array value case not cross page. */
static void InsertArrayValueNotCrossPage(Refer *refer, MetaColumn *meta_column, List *values, List *bound) {
    Buffer buffer;
    void *block, *dest;
    ListCell *lc1, *lc2;
    size_t offset;
    uint32_t row_num;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    offset = 0;

    /* Assign bound meta info. */
    foreach(lc1, bound) {
        memcpy(dest + offset, lfirst(lc1), sizeof(int));
        offset += sizeof(int);
    }

    /* Assign array values. */
    foreach (lc2, values) {
        memcpy(dest + offset, lfirst(lc2), meta_column->column_length);
        offset += meta_column->column_length;
    }
    
    /* Update refer. */
    row_num = offset / ARRAY_TABLE_ROW_SIZE;
    if (row_num % ARRAY_TABLE_ROW_SIZE != 0) row_num++;
    refer->cell_num += row_num;
    if (refer->cell_num == ARRAY_TABLE_ROW_NUM) {
        /* If current page is full, move to next page and first cell. */
        refer->page_num++;
        refer->cell_num = ARRAY_TABLE_FIRST_NUM;
    }
    
    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert array value inner. 
 * refer:       The refer of array value will be posted.
 * array:       Array value.
 * meta_column: Meta column.
 * Return:      The refer the array value posted.
 * */
static void InsertArrayValueInner(Refer *refer, ArrayValue *array, MetaColumn *meta_column) {
    List *values, *bound;
    Size size;

    values = create_list(NODE_VOID);
    bound = create_list(NODE_INT);
    
    CalcArrayValueValues(array, meta_column->array_dim, values);
    CalcArrayValueBound(array, meta_column, bound);
    size = CalcArrayValueBoundSize(meta_column, bound);
    
    if (ArrayTablePageWillOverflow(refer, size)) 
        /* Not cross page. */
        InsertArrayValueCrossPage(refer, meta_column, values, bound);
    else 
        /* Cross page. */
        InsertArrayValueNotCrossPage(refer, meta_column, values, bound);
}

/* Insert array value. 
 * oid:         Array heap table oid.
 * array:       Array value.
 * meta_column: Meta column.
 * Return:      The refer the array value posted.
 * */
Refer *InsertArrayValue(Oid oid, ArrayValue *array, MetaColumn *meta_column) {
    Buffer buffer;
    void *block;
    Refer *rrefer, *nrefer;

    Assert(meta_column->array_dim > 0);
    AssertFalse(ZERO_OID(oid));

    buffer = ReadBuffer(oid, ARRAY_TABLE_ROOT_PAGE);
    LockBuffer(buffer, RW_WRITER);

    block = GetBufferPage(buffer);
    rrefer = GetRootRefer(block);
    Assert(rrefer->oid == oid);
    nrefer = copy_refer(rrefer);
    
    /* Insert array value. */
    InsertArrayValueInner(nrefer, array, meta_column);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return nrefer;
}

/* Drop the array value table vai table name
 * table_name:  Table name.
 * Return:      Success or fail.
 * . */
bool DropArrayHeapTable(char *table_name) {
    Oid oid;
    char *str_table_file;

    oid = ArrayTableNameFindOid(table_name);
    AssertFalse(ZERO_OID(oid));
    str_table_file = table_file_path(oid);

    if (!check_table_exist_direct(oid)) {
        logger(ERROR, "Table file '%s' not exists, error : %s", 
               str_table_file, strerror(errno));
        return false;
    }

    /* Delete physically. */
    if (remove(str_table_file) == 0 && RemoveObject(oid)) {
        /* Unregister fdesc. */
        unregister_fdesc(oid);
        return true;
    }

    /* Not reach here logically. */
    UNREACHABLE(false, "Try to drop array heap table '%s' fail, error : %s", 
                table_name, strerror(errno));
}

