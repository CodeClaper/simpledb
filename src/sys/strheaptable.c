#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "strheaptable.h"
#include "bufpool.h"
#include "data.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "bufmgr.h"
#include "systable.h"

/* Create the string heap table. */
bool CreateStrHeapTable(char *table_name) {
    int descr;
    Size w_size;
    void *rblock;
    Refer *rRefer;
    char str_table_file[MAX_TABLE_NAME_LEN + 100];
    Object entity;
    
    entity = GenerateObject(table_name, OSTRING_HEAP_TABLE);
    memset(str_table_file, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(str_table_file, "%s%ld", conf->data_dir, entity.oid);

    /* Avoid repeatly create. */
    if (table_file_exist(str_table_file))
        return true;

    descr = open(str_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(PANIC, "Open database file '%s' fail.\n", str_table_file);
        return false;
    }
    
    rblock = dalloc(PAGE_SIZE);
    rRefer = new_refer(entity.oid, 0, 1);
    memcpy(rblock, rRefer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, rblock, PAGE_SIZE);
    if (w_size == -1) {
        db_log(PANIC, "Write table meta info error and errno %d.\n", errno);
        return false;
    } 

    /* Save the String table Object. */
    Assert(SaveObject(entity));
    
    /* Free memory. */
    dfree(rblock);
    dfree(rRefer);

    /* Close desription. */
    close(descr);

    return true;
}

static Refer *GetRootRefer(void *root_node) {
    Refer *root_refer = instance(Refer);
    memcpy(root_refer, root_node, sizeof(Refer));
    return root_node;
}

/* Insert without cross page. */
static void InsertNotCrossPage(Refer *rRefer, char *strVal) {
    Size size;
    Buffer buffer;
    void *block;
    uint32_t useRowNum;

    size = strlen(strVal) + 1;
    buffer = ReadBuffer(rRefer->oid, rRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);

    useRowNum = size / STRING_ROW_SIZE;
    if (size % STRING_ROW_SIZE != 0)
        useRowNum++;
    Assert(rRefer->cell_num + useRowNum <= STRING_ROW_SIZE);
    
    /* Store the string value. */
    memcpy(block + rRefer->cell_num * STRING_ROW_SIZE, strVal, size);

    /* Update row refer. */
    rRefer->cell_num += useRowNum;

    MakeBufferDirty(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert cross page. */
static void InsertCrossPage(Refer *rRefer, char *strVal) {
    Buffer buffer;
    uint32_t leftRowNum;
    void *block, *pointer;
    Size size, leftSize, useSize;

    size = strlen(strVal) + 1;
    leftRowNum = STRING_ROW_NUM - rRefer->cell_num;
    leftSize = leftRowNum * STRING_ROW_SIZE;
    buffer = ReadBuffer(rRefer->oid, rRefer->page_num);
    LockBuffer(buffer, RW_WRITER);
    block = GetBufferBlock(buffer);
    pointer = block + rRefer->cell_num * STRING_ROW_SIZE;
    
    /* Store the string value to current page. */
    memcpy(pointer, strVal, leftSize);
    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    /* Store the string value to next page. */
    useSize = leftSize;
    while (useSize < size) {
        Buffer nbuffer;
        void *nblock;

        nbuffer = ReadBuffer(rRefer->oid, ++(rRefer->page_num));
        LockBuffer(nbuffer, RW_WRITER);
        nblock = GetBufferPage(nbuffer);

        leftSize = size - useSize;
        if (leftSize <= PAGE_SIZE) {
            leftRowNum = leftSize / STRING_ROW_SIZE;
            if (leftSize % STRING_ROW_SIZE != 0)
                leftRowNum++;
            memcpy(nblock, strVal + useSize, leftSize);
            rRefer->cell_num = leftRowNum;
            useSize = size;
        } else {
            memcpy(nblock, strVal + useSize, PAGE_SIZE);
            useSize += PAGE_SIZE;
        }

        MakeBufferDirty(nbuffer);
        UnlockBuffer(nbuffer);
        ReleaseBuffer(nbuffer);
    }
}

/* Update the root refer. */
static void InsertStringValueInner(Refer *rRefer, char *strVal) {
    Size size;
    uint32_t useRowNum, leftRowNum;
    
    size = strlen(strVal) + 1;
    leftRowNum = STRING_ROW_NUM - rRefer->cell_num;
    useRowNum = size / STRING_ROW_SIZE;
    if (size % STRING_ROW_SIZE != 0)
        useRowNum++;

    if (leftRowNum >= useRowNum) 
        /* Not cross page. */
        InsertNotCrossPage(rRefer, strVal);
    else 
        /* cross page. */
        InsertCrossPage(rRefer, strVal);
}


/* Insert new String value. 
 * Return the Refer value.
 * */
StrRefer *InsertStringValue(Oid oid, char *str_val) {
    Size size;
    Buffer rbuffer;
    void *root;
    Refer *rRefer;
    StrRefer *strRefer;

    size = strlen(str_val) + 1;
    rbuffer = ReadBuffer(oid, STRING_TABLE_ROOT_PAGE);
    LockBuffer(rbuffer, RW_WRITER);
    root = GetBufferPage(rbuffer);
    rRefer = GetRootRefer(root);
    
    /* Get StrRefer. */
    strRefer = instance(StrRefer);
    memcpy(&strRefer->refer, rRefer, sizeof(Refer));
    strRefer->size = size; 

    /* Insert String value. */
    InsertStringValueInner(rRefer, str_val);

    MakeBufferDirty(rbuffer);
        
    dfree(rRefer);
    UnlockBuffer(rbuffer);
    ReleaseBuffer(rbuffer);

    return strRefer;
}

/* Overflow the page. */
static inline bool OverflowPage(StrRefer *strRefer) {
    return strRefer->refer.cell_num * STRING_ROW_SIZE + strRefer->size <= PAGE_SIZE;
}

/* Query not cross page. */
static char *QueryNotCrossPage(StrRefer *strRefer) {
    char *strVal;
    Buffer buffer;
    Refer rRefer;
    void *block;
    
    rRefer = strRefer->refer;
    buffer = ReadBuffer(rRefer.oid, rRefer.page_num);
    LockBuffer(buffer, RW_READERS);
    block = GetBufferPage(buffer);

    strVal = dalloc(strRefer->size);
    memcpy(strVal, block + rRefer.cell_num * STRING_ROW_SIZE, strRefer->size);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return strVal;
}

/* Query cross page. */
static char *QueryCrossPage(StrRefer *strRefer) {
    char *strVal;
    uint32_t leftSize, useSize, currentPageNum;
    Refer rRefer;
    Buffer rbuffer;
    void *rblock;

    rRefer = strRefer->refer;
    currentPageNum = rRefer.page_num;
    useSize = 0;
    leftSize = PAGE_SIZE - rRefer.cell_num * STRING_ROW_SIZE;
    Assert(leftSize < strRefer->size);
    strVal = dalloc(strRefer->size);

    /* Read current page. */
    rbuffer = ReadBuffer(rRefer.oid, currentPageNum);
    LockBuffer(rbuffer, RW_READERS);
    rblock = GetBufferPage(rbuffer);
    memcpy(strVal + useSize, rblock + rRefer.cell_num * STRING_ROW_SIZE, leftSize);
    UnlockBuffer(rbuffer);
    ReleaseBuffer(rbuffer);

    /* Read next page. */
    useSize += leftSize;
    while (useSize < strRefer->size) {
        Buffer nbuffer;
        void *nblock;

        nbuffer = ReadBuffer(rRefer.oid, ++currentPageNum);
        LockBuffer(nbuffer, RW_READERS);
        nblock = GetBufferPage(nbuffer);

        leftSize = strRefer->size - useSize;
        if (leftSize <= PAGE_SIZE) {
            memcpy(strVal + useSize, nblock, leftSize);
            useSize = strRefer->size;
        } else {
            memcpy(strVal + useSize, nblock, PAGE_SIZE);
            useSize += PAGE_SIZE;
        }

        UnlockBuffer(nbuffer);
        ReleaseBuffer(nbuffer);
    }

    return strVal;
}


/* Query string value. */
char *QueryStringValue(StrRefer *strRefer) {
    return OverflowPage(strRefer) 
        ? QueryNotCrossPage(strRefer) : QueryCrossPage(strRefer);
}

/* Drop the string heap table. */
bool DropStrHeapTable(char *table_name) {
    Oid oid;
    char *str_table_file;

    oid = StrTableNameFindOid(table_name);
    AssertFalse(ZERO_OID(oid));
    str_table_file = table_file_path(oid);

    if (!check_table_exist_direct(oid)) {
        db_log(ERROR, "Table file '%s' not exists, error : %s", 
               str_table_file, strerror(errno));
        return false;
    }

    /* Delete physically. */
    if (remove(str_table_file) == 0 && RemoveObject(oid))
        return true;

    /* Not reach here logically. */
    db_log(ERROR, 
           "String heap table '%s' deleted fail, error : %s", 
           table_name, strerror(errno));

    return false;
}
