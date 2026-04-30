#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include "arrheaptable.h"
#include "systable.h"
#include "table.h"
#include "log.h"

/* Create array heap table inner.
 * oid:         Array heap table oid.
 * */
static bool CreateArrHeapTableInner(Oid oid) {
    int descr;
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

    return false;
}

/* Create array heap table.
 * oid:         Array heap table oid.
 * tid:         Table oid.
 * table_name:  Table name.
 * Return:      Success or fail. 
 * */
bool CreateArrHeapTable(Oid oid, Oid tid, char *table_name) {
    Object entity = GenerateObjectInner(oid, tid, table_name, OARRAY_HEAP_TABLE);
    return CreateArrHeapTableInner(oid) && SaveObject(entity);
}

/* Query array value. */
ArrayValue *QueryArrayValue(Refer *refer, int dim) {
    return NULL;
}

/* Insert array value. */
void InsertArrayValue(ArrayValue *array, int dim) {

}

/* Drop array value. */
bool DropArrHeapTable(Oid oid) {
    return false;
}

