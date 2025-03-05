#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include "bufpool.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "data.h"
#include "fdesc.h"
#include "ltree.h"
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
    void *page; 

    fdesc = get_file_desc(tag->tableName);
    page = GetBufferBlock(buffer);

    lseek(fdesc, tag->blockNum * PAGE_SIZE, SEEK_SET);
    ssize_t read_bytes = read(fdesc, page, PAGE_SIZE);
    if (read_bytes == -1) {
        fprintf(stderr, "Table file read error: %s", strerror(errno));
        exit(1);
    }
}

/* Write Buffer Block. */
void BufferWriteBlock(Buffer buffer) {
    FDesc fdesc;
    BufferTag tag;
    BufferDesc *desc;
    void *node;

    desc = GetBufferDesc(buffer);
    tag = desc->tag;
    fdesc = get_file_desc(tag.tableName);
    node = GetBufferBlock(buffer);

    /* Only flush dirty page. */
    if (get_node_state(node) != DIRTY_STATE)
        return;

    off_t offset = lseek(fdesc, PAGE_SIZE * tag.blockNum, SEEK_SET);
    if (offset == (off_t)-1) {
        db_log(PANIC, "Error seek set: %s, which happen in %s and page num %d.", 
               strerror(errno), tag.tableName, tag.blockNum);
        exit(1);
    }

    /* Write. */
    ssize_t write_size = write(fdesc, node, PAGE_SIZE);
    if (write_size == -1) {
        db_log(PANIC, "Try to write page error: %s.", strerror(errno));
        exit(1);
    }

    fsync(fdesc);
}
