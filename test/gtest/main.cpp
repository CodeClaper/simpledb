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

    /* Initialise transaction. */
    InitTrans();

    /* Initialise table cache. */
    InitTableCache();

    /* Initialise refer. */
    init_refer();

    /* Load configuration. */
    conf = load_conf();
}

int main(int argc, char **argv) {
    db_start();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
