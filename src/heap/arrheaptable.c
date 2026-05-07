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
#define ARRAY_TABLE_META_SIZE   ARRAY_TABLE_ROW_SIZE
#define ARRAY_TABLE_ROW_SIZE    (PAGE_SIZE / ARRAY_TABLE_ROW_NUM)
#define ARRAY_TABLE_DATA_SIZE   (PAGE_SIZE - ARRAY_TABLE_ROW_SIZE)


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

    return useRowNum > leftRowNum;
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


/* Calculate values size. 
 * If the array is 3D array, the M x N x P is value size.
 * meta_column: Meta column.
 * bound:       bound.
 * Return:      Values size.
 * */
static Size CalcArrayValueValuesSize(MetaColumn *meta_column, List *bound) {
    Size size = 1;
    ListCell *lc;
    foreach (lc, bound) {
        size = size * lfirst_int(lc);
    }
    return size * meta_column->column_length;
}

/* Calculate bound size. 
 * Bound size = values size + dim size;
 * If the array is 3D array, the M x N x P is value size, and three int size is the dim size.
 * meta_column: Meta column.
 * bound:       bound.
 * Return:      Bound size.
 * */
static Size CalcArrayValueBoundSize(MetaColumn *meta_column, List *bound) {
    return (sizeof(int) * bound->size) + CalcArrayValueValuesSize(meta_column, bound);
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

/* Calculate array values tiled src. 
 * meta_column: Meta Column.
 * values:      Values;
 * bound:       Bound;
 * Return:      Tiled src.
 * */
static void *CalcArrayValueTiledSrc(MetaColumn *meta_column, List *values, List *bound) {
    ListCell *lc;
    size_t offset, values_size;
    void *src;
    
    offset = 0;
    values_size = CalcArrayValueValuesSize(meta_column, bound);
    src = dalloc(values_size);

    foreach (lc, values) {
        memcpy(src + offset, lfirst(lc), meta_column->column_length);
        offset += meta_column->column_length;
    }

    return src;
}

/* Insert array value case cross page. */
static void InsertArrayValueCrossPage(Refer *refer, MetaColumn *meta_column, List *values, List *bound) {
    Buffer buffer;
    void *block, *dest, *tiled_src;
    size_t offset, size, left_size, use_size;
    uint32_t left_row_num;
    int scope;

    size = CalcArrayValueValuesSize(meta_column, bound);
    left_row_num = ARRAY_TABLE_ROW_NUM - refer->cell_num;
    left_size = left_row_num * ARRAY_TABLE_ROW_SIZE;
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    offset = 0; 
    tiled_src = CalcArrayValueTiledSrc(meta_column, values, bound);

    /* Assign bound meta info. */
    ListCell *lc;
    foreach (lc, bound) {
        scope = lfirst_int(lc);
        memcpy(dest + offset, &scope, sizeof(int));
        offset += sizeof(int);
    }

    left_size -= offset;
    memcpy(dest + offset, tiled_src, left_size);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    refer->cell_num = ARRAY_TABLE_ROW_NUM;

    /* Store the array value to next page. */
    use_size = left_size;
    while (use_size < size) {
        Buffer nbuffer;
        void *nblock;

        nbuffer = ReadBuffer(refer->oid, ++(refer->page_num));
        LockBuffer(nbuffer, RW_WRITER);
        nblock = GetBufferPage(nbuffer);

        left_size = size - use_size;
        /* Check if next page can store the left tiled data completely. 
         * Note: not the whole page to store rather than the remaining part after 
         * exclusing the first ARRAY_TABLE_ROW_SIZE part. */
        if (left_size <= ARRAY_TABLE_DATA_SIZE) {
            left_row_num = left_size / ARRAY_TABLE_ROW_SIZE;
            if (left_size % ARRAY_TABLE_ROW_SIZE != 0) left_row_num++;
            memcpy((nblock + ARRAY_TABLE_META_SIZE), tiled_src + use_size, left_size);
            refer->cell_num = left_row_num + 1;
            use_size = size;
        } else {
            memcpy(nblock + ARRAY_TABLE_META_SIZE, tiled_src + use_size, ARRAY_TABLE_DATA_SIZE);
            use_size += PAGE_SIZE;
        }

        /* If current page is full, move to next page and first cell. */
        if (refer->cell_num == ARRAY_TABLE_ROW_NUM) {
            refer->page_num++;
            refer->cell_num = ARRAY_TABLE_FIRST_NUM;
        }

        MakeBufferDirty(nbuffer);
        UnlockBuffer(nbuffer);
        ReleaseBuffer(nbuffer);
    }
    
    dfree(tiled_src);
}

/* Insert array value case not cross page. */
static void InsertArrayValueNotCrossPage(Refer *refer, MetaColumn *meta_column, List *values, List *bound) {
    Buffer buffer;
    void *block, *dest;
    ListCell *lc1, *lc2;
    size_t offset;
    uint32_t row_num;
    int scope;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    offset = 0;

    /* Assign bound meta info. */
    foreach (lc1, bound) {
        scope = lfirst_int(lc1);
        memcpy(dest + offset, &scope, sizeof(int));
        offset += sizeof(int);
    }

    /* Assign array values. */
    foreach (lc2, values) {
        memcpy(dest + offset, lfirst(lc2), meta_column->column_length);
        offset += meta_column->column_length;
    }
    
    /* Update refer. */
    row_num = offset / ARRAY_TABLE_ROW_SIZE;
    if (offset % ARRAY_TABLE_ROW_SIZE != 0) row_num++;
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
    
    CalcArrayValueValues(array, meta_column->array_dim - 1, values);
    CalcArrayValueBound(array, meta_column, bound);
    size = CalcArrayValueBoundSize(meta_column, bound);
    
    if (ArrayTablePageWillOverflow(refer, size)) 
        /* Not cross page. */
        InsertArrayValueCrossPage(refer, meta_column, values, bound);
    else 
        /* Cross page. */
        InsertArrayValueNotCrossPage(refer, meta_column, values, bound);

    free_list(values);
    free_list(bound);
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
    InsertArrayValueInner(rrefer, array, meta_column);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return nrefer;
}

/* Query array value bound. */
static List *QueryArrayValueBound(Refer *refer, MetaColumn *meta_column) {
    int i;
    Buffer buffer;
    void *block, *dest;
    List *bound;
    size_t offset;
    
    bound = create_list(NODE_INT);
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    offset = 0;
    
    for (i = 0; i < meta_column->array_dim; i++) {
        append_list(bound, dest + offset);
        offset += sizeof(int);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return bound;
}

/* Query and loop array value. */
static void QueryAndLoopArrayValue(ArrayValue *arr, void *dest, MetaColumn *meta_column, List *bound, int idx, size_t *offset) {
    int num = lfirst_int(list_nth_cell(bound, idx));
    for (int i = 0; i < num; i++) {
        if (idx > 0) {
            ArrayValue *child = new_array_value(meta_column->column_type, 3);
            QueryAndLoopArrayValue(child, dest, meta_column, bound, idx - 1, offset);
            append_list(arr->list, child);
        } else {
            append_list(arr->list, dest + (*offset));
            (*offset) += meta_column->column_length;
        }
    }
}

/* Query Array value tiled dest. */
static void *QueryArrayValueTiledDest(Refer *refer, MetaColumn *meta_column, List *bound) {
    Buffer buffer;
    Size values_size;
    void *block, *src, *dest;
    uint32_t left_size, use_size;
    int32_t current_page;

    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferPage(buffer);
    src = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE + sizeof(int) * meta_column->array_dim;
    values_size = CalcArrayValueValuesSize(meta_column, bound);
    dest = dalloc(values_size);
    current_page = refer->page_num;

    left_size = PAGE_SIZE - refer->cell_num * ARRAY_TABLE_ROW_SIZE;
    memcpy(dest, src, left_size);
    use_size = left_size;

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    while (use_size < values_size) {
        Buffer nbuffer;
        void *nblock;

        nbuffer = ReadBuffer(refer->oid, ++current_page);
        LockBuffer(nbuffer, RW_READERS);
        nblock = GetBufferPage(nbuffer);

        left_size = values_size - use_size;
        if (left_size <= ARRAY_TABLE_DATA_SIZE) {
            memcpy(dest + use_size, (nblock + ARRAY_TABLE_META_SIZE), left_size);
            use_size = values_size;
        } else {
            memcpy(dest + use_size, (nblock + ARRAY_TABLE_META_SIZE), ARRAY_TABLE_DATA_SIZE);
            use_size += PAGE_SIZE;
        }

        UnlockBuffer(nbuffer);
        ReleaseBuffer(nbuffer);
    }

    return dest;
} 

/* Query array value cross page. */
static ArrayValue *QueryArrayValueCrossPage(Refer *refer, MetaColumn *meta_column, List *bound) {
    ArrayValue *arr;
    void *dest;
    size_t offset;
    
    arr = new_array_value(meta_column->column_type, 3);
    dest = QueryArrayValueTiledDest(refer, meta_column, bound);
    offset = 0;

    QueryAndLoopArrayValue(arr, dest, meta_column, bound, meta_column->array_dim - 1, &offset);
    dfree(dest);

    return arr;
}


/* Query array value without cross page. */
static ArrayValue *QueryArrayValueNotCrossPage(Refer *refer, MetaColumn *meta_column, List *bound) {
    ArrayValue *arr;
    Buffer buffer;
    void *block, *dest;
    size_t offset;

    arr = new_array_value(meta_column->column_type, 3);
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferPage(buffer);
    dest = block + refer->cell_num * ARRAY_TABLE_ROW_SIZE + sizeof(int) * meta_column->array_dim;
    offset = 0;

    QueryAndLoopArrayValue(arr, dest, meta_column, bound, meta_column->array_dim - 1, &offset);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    return arr;
}

/* Query array value. */
ArrayValue *QueryArrayValue(Refer *refer, MetaColumn *meta_column) {
    List *bound;
    Size size;

    Assert(refer != NULL);
    Assert(meta_column->array_dim > 0);

    bound = QueryArrayValueBound(refer, meta_column);
    size = CalcArrayValueBoundSize(meta_column, bound);

    if (ArrayTablePageWillOverflow(refer, size)) return QueryArrayValueCrossPage(refer, meta_column, bound);
    else return QueryArrayValueNotCrossPage(refer, meta_column, bound);
}


/* Drop the array value table vai table name
 * table_name:  Table name.
 * Return:      Success or fail.
 * */
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

