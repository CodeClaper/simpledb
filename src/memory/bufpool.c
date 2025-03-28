#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "bufpool.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "data.h"
#include "fdesc.h"
#include "ltree.h"
#include "table.h"
#include "compres.h"
#include "log.h"

/*
 * The Buffer Pool. 
 */
static void *BufferPool;

/* Create the Buffer Pool. */
void CreateBufferPool() {
    switch_shared();
    BufferPool = dalloc(PAGE_SIZE * BUFFER_SLOT_NUM);
    switch_local();
}

/* Get Block. */
inline void *GetBufferBlock(Buffer buffer) {
   return (void *)(((char *) BufferPool) + PAGE_SIZE * buffer);
}

/* Read Buffer Block. */
void BufferReadBlock(BufferTag *tag, Buffer buffer) {
    FDesc fdesc; 
    void *block; 
    char compres[ACTUAL_PAGE_SIZE];

    memset(compres, 0, ACTUAL_PAGE_SIZE);
    fdesc = get_file_desc(tag->tableName);
    block = GetBufferBlock(buffer);

    lseek(fdesc, tag->blockNum * ACTUAL_PAGE_SIZE, SEEK_SET);
    ssize_t read_bytes = read(fdesc, compres, ACTUAL_PAGE_SIZE);
    if (read_bytes == -1) {
        fprintf(stderr, "Table file read error: %s", strerror(errno));
        exit(1);
    }

    /* Note that: we need to confirm it`s first to load block from pager. 
     * We use whether `compres` is equal empty to determine if it is loding 
     * from pager for the first time. */
    if (streq(compres, ""))
        /* Not uncompress, just initialization. */
        memset(block, 0, PAGE_SIZE);
    else
        /* Uncompress. */
        Uncompress(compres, block);
}

/* Write Buffer Block. */
void BufferWriteBlock(Buffer buffer) {
    FDesc fdesc;
    BufferTag tag;
    BufferDesc *desc;
    void *block;

    desc = GetBufferDesc(buffer);
    tag = desc->tag;
    block = GetBufferBlock(buffer);

    /* Only flush dirty page. */
    if (get_node_state(block) != DIRTY_STATE)
        return;

    /* Maybe table has dropped, so necessary 
     * to check if table still exists. */
    if (!check_table_exist(tag.tableName))
        return;

    fdesc = get_file_desc(tag.tableName);

    off_t offset = lseek(fdesc, ACTUAL_PAGE_SIZE * tag.blockNum, SEEK_SET);
    if (offset == (off_t)-1) {
        db_log(PANIC, "Error seek set: %s, which happen in %s and page num %d.", 
               strerror(errno), tag.tableName, tag.blockNum);
        exit(1);
    }

    /* Compress data. */
    void *compres = Compress(block);

    /* Write. */
    ssize_t write_size = write(fdesc, compres, ACTUAL_PAGE_SIZE);
    if (write_size == -1) {
        db_log(PANIC, "Try to write page error: %s.", strerror(errno));
        exit(1);
    }

    fsync(fdesc);

    dfree(compres);
}
