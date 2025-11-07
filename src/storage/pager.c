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
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "pager.h"
#include "spinlock.h"
#include "fdesc.h"
#include "mmgr.h"

/* Get the page size. */
Size GetPageSize(Oid oid) {
    FDesc fdesc;
    off_t file_length;
    fdesc = get_file_desc(oid);

    file_length = lseek(fdesc, 0, SEEK_END);
    if (file_length == -1) {
        fprintf(stderr, "Error seek end: %s.", strerror(errno));
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

