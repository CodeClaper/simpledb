#include "gtest/gtest.h"
#include <setjmp.h>

extern "C" {
#include "defs.h"
#include "data.h"
#include "shmem.h"
#include "shmgr.h"
#include "log.h"
#include "session.h"
#include "trans.h"
#include "xlog.h"
#include "tablecache.h"
#include "conf.h"
#include "refer.h"
#include "mctx.h"
#include "bufmgr.h"
#include "tablelock.h"
#include "systable.h"
#include "fdesc.h"
}
Conf *conf; /* Conf */
jmp_buf errEnv; /* jmp_buf for error. */
const char *program_name;  

/* DB Start. */
static void db_start() {

    /* MemoryContext init.*/
    MemoryContextInit();

    /* Initialise shmem. */
    init_shmem();

    /* Initialise memory manger. */
    init_mem();

    /* Initialise fesc.*/
    init_fdesc();


    /* Initialise transaction. */
    InitTrans();

    /* Initialise bufmgr. */
    InitBufMgr();

    /* Initialise refer. */
    init_refer();

    /* Initialise table lock. */
    init_table_lock();

    /* Load configuration. */
    conf = load_conf();

    /* Initialise table cache. */
    InitTableCache();

    /* Init system table. */
    InitSysTable();
}

int main(int argc, char **argv) {
    db_start();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
