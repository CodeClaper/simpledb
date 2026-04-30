#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "arrheaptable.h"
#include "instance.h"
#include "systable.h"
#include "table.h"
#include "mmgr.h"
#include "log.h"
#include "fdesc.h"

#define ARRAY_TABLE_ROOT_PAGE 0
#define ARRAY_TABLE_FIRST_NUM 1

/* Create array heap table inner.
 * oid:         Array heap table oid.
 * Return:      Success or fail.
 * */
static bool CreateArrayHeapTableInner(Oid oid) {
    int descr, w_size;
    void *block;
    Refer *refer;
    char file_path[MAX_TABLE_NAME_LEN + 100];
    
    memset(file_path, 0, MAX_TABLE_NAME_LEN + 100);
    sprintf(file_path, "%s%ld", conf->data_dir, oid);

    /* Avoid repeatly create. */
    if (table_file_exist(file_path)) {
        THROW("String heap table file %s alreay exists.", file_path);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        THROW("Open database file '%s' fail.", file_path);
        return false;
    }

    /* Initialize page. */
    block = dalloc(PAGE_SIZE);
    refer = new_refer(oid, ARRAY_TABLE_ROOT_PAGE, ARRAY_TABLE_FIRST_NUM);
    memcpy(block + NODE_STATE_SIZE, refer, sizeof(Refer));

    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, block, PAGE_SIZE);
    if (w_size == -1) {
        THROW("Write table meta info error and error message: %s.", strerror(errno));
        return false;
    } 

    dfree(block);
    dfree(refer);
    close(descr);

    return true;
}

/* Create array heap table.
 * oid:         Array heap table oid.
 * tid:         Table oid.
 * table_name:  Table name.
 * Return:      Success or fail. 
 * */
bool CreateArrayHeapTable(Oid oid, Oid tid, char *table_name) {
    Object entity = GenerateObjectInner(oid, tid, table_name, OARRAY_HEAP_TABLE);
    return CreateArrayHeapTableInner(oid) && SaveObject(entity);
}

/* Query array value. */
ArrayValue *QueryArrayValue(Refer *refer, int dim) {
    return NULL;
}

/* Insert array value. */
void InsertArrayValue(ArrayValue *array, int dim) {

}

/* Drop array value. */
bool DropArrayHeapTable(char *table_name) {
    Oid oid;
    char *str_table_file;

    oid = ArrayTableNameFindOid(table_name);
    AssertFalse(ZERO_OID(oid));
    str_table_file = table_file_path(oid);

    if (!check_table_exist_direct(oid)) {
        logger(ERROR, "Table file '%s' not exists, error : %s", 
               str_table_file, strerror(errno));
        return false;
    }

    /* Delete physically. */
    if (remove(str_table_file) == 0 && RemoveObject(oid)) {
        /* Unregister fdesc. */
        unregister_fdesc(oid);
        return true;
    }

    /* Not reach here logically. */
    UNREACHABLE(false, "Try to drop array heap table '%s' fail, error : %s", 
                table_name, strerror(errno));
}

