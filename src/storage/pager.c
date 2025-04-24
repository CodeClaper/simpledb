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

static s_lock *plock;

/* Init Pager*/
void InitPager() {
    switch_shared();
    plock = instance(s_lock);
    init_spin_lock(plock);
    switch_local();
}

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
    uint32_t page_num;
    acquire_spin_lock(plock);
    page_num = table->page_size++;
    release_spin_lock(plock);
    return page_num;
}

