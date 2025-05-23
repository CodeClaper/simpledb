#include <stdbool.h>
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

    fdesc = get_file_desc(tag->oid);
    block = GetBufferBlock(buffer);

    lseek(fdesc, tag->blockNum * PAGE_SIZE, SEEK_SET);
    ssize_t read_bytes = read(fdesc, block, PAGE_SIZE);
    if (read_bytes == -1) {
        fprintf(stderr, "Table file read error: %s", strerror(errno));
        exit(1);
    }
}

/* Write Buffer Block. 
 * If realy write to file, return true.
 * */
bool BufferWriteBlock(Buffer buffer) {
    FDesc fdesc;
    BufferTag tag;
    BufferDesc *desc;
    void *block;

    desc = GetBufferDesc(buffer);
    tag = desc->tag;
    block = GetBufferBlock(buffer);

    /* Only flush dirty page. */
    if (get_node_state(block) != DIRTY_STATE)
        return false;

    /* Maybe table has dropped before calling the function, 
     * so necessary to check if table still exists. */
    if (!check_table_exist_direct(tag.oid))
        return false;

    fdesc = get_file_desc(tag.oid);

    off_t offset = lseek(fdesc, PAGE_SIZE * tag.blockNum, SEEK_SET);
    if (offset == (off_t) -1) {
        db_log(PANIC, "Error seek set: %s, which happen in %ld and page num %d.", 
               strerror(errno), tag.oid, tag.blockNum);
        exit(1);
    }

    /* Write. */
    ssize_t write_size = write(fdesc, block, PAGE_SIZE);
    if (write_size == -1) {
        db_log(PANIC, "Try to write page error: %s.", strerror(errno));
        exit(1);
    }

    fsync(fdesc);

    /* If write success, make the buffer NORMAL 
     * to avoid scan it again. */
    MakeBufferNormal(desc->buffer);

    return true;
}
