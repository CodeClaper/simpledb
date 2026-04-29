/********************************** Pager Manager Module **************************************************
 * Auth:        JerryZhou
 * Created:     2023/12/19
 * Modify:      2024/11/26
 * Locataion:   src/storage/pager.c
 * Description: The pager manager supports the ways to manipulate the disk file data.
 * One page is the basic unit of which data is store/read/write to disk and memory.
 * PAGE_SIZE is the size of a page and data length of each IO operation.
 * Not recommand that you get data via the pager manager direactly, but the buffer manager indireactly.
 ***********************************************************************************************************
 */

#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "pager.h"
#include "data.h"
#include "spinlock.h"
#include "fdesc.h"
#include "mmgr.h"
#include "bufmgr.h"
#include "log.h"

/* Get the page size. */
Size GetPageSize(Oid oid) {
    FDesc fdesc;
    off_t file_length;
    fdesc = get_file_desc(oid);

    file_length = lseek(fdesc, 0, SEEK_END);
    if (file_length == -1) {
        logger(PANIC, "Error seek end: %s.", strerror(errno));
        exit(1);
    }
    
    return (file_length / ACTUAL_PAGE_SIZE);
}

/* Get next page num. */
uint32_t GetNextUnusedPageNum(Table *table) {
    uint32_t page_num = table->page_size;
    while (!__sync_bool_compare_and_swap(&table->page_size, page_num, page_num + 1)) {
        page_num = table->page_size;
    }
    return page_num;
}

/* Get next rid page num. */
uint32_t GetNextUnusedRidPageNum(Table *table) {
    uint32_t page_num = table->rid_page_size;
    while (!__sync_bool_compare_and_swap(&table->rid_page_size, page_num, page_num + 1)) {
        page_num = table->rid_page_size;
    }
    return page_num;
}

/* Get next sid page num. */
uint32_t GetNextUnusedSidPageNum(Table *table) {
    uint32_t page_num = table->sid_page_size;
    while (!__sync_bool_compare_and_swap(&table->sid_page_size, page_num, page_num + 1)) {
        page_num = table->sid_page_size;
    }
    return page_num;
}

/* Reset Page. */
void ResetPage(Oid oid, uint32_t page_num) {
    Buffer buffer;
    void *node;

    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    node = GetBufferPage(buffer);

    memset(node, 0, PAGE_SIZE);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}
