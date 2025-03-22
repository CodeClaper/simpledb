#include <zlib.h>
#include "data.h"
#include "compres.h"
#include "mmgr.h"
#include "log.h"


/* Compress the block. */
void *Compress(void *block) {
    int ret;
    uLong dest_len; 
    void *compr;

    dest_len = compressBound(PAGE_SIZE);   
    compr = dalloc(dest_len);
    ret = compress2(compr, &dest_len, block, (uLong) PAGE_SIZE, Z_DEFAULT_COMPRESSION);

    if (ret == Z_MEM_ERROR)
        db_log(PANIC, "Try to compress fail, out of memory.");

    if (ret == Z_DATA_ERROR)
        db_log(PANIC, "Try to compress fail, bad input data.");

    if (ret != Z_OK) 
        db_log(PANIC, "Try to compress fail.");
    
    if (dest_len > ACTUAL_PAGE_SIZE)
        db_log(PANIC, "Exceed the compress page size: %ld > %ld", dest_len, ACTUAL_PAGE_SIZE);

    return compr;
}

/* Uncompress to block. */
void Uncompress(void *compr, void *block) {
    int ret;
    uLong dest_len;

    ret = uncompress(block, &dest_len, compr, ACTUAL_PAGE_SIZE);

    if (ret == Z_MEM_ERROR)
        db_log(PANIC, "Try to uncompress fail, out of memory.");

    if (ret == Z_DATA_ERROR)
        db_log(PANIC, "Try to uncompress fail, bad input data.");

    if (ret != Z_OK) 
        db_log(PANIC, "Try to uncompress fail.");

    if (dest_len > PAGE_SIZE)
        db_log(PANIC, "Exceed the uncompress page size: %ld > %ld", dest_len, PAGE_SIZE);
}
