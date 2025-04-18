#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "strheaptable.h"
#include "data.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "bufmgr.h"

/* Create the string heap table. */
bool CreateStrHeapTable(char *table_name) {
    int descr;
    Size w_size;
    void *root_node;
    Refer *root_refer;
    char str_table_file[MAX_TABLE_NAME_LEN + 100];

    memset(str_table_file, 0, MAX_TABLE_NAME_LEN + 10);
    sprintf(str_table_file, "%s%s%s", conf->data_dir, table_name, ".dbs");
    
    /* Avoid repeatly create. */
    if (table_file_exist(str_table_file))
        return true;

    descr = open(str_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(PANIC, "Open database file '%s' fail.\n", str_table_file);
        return false;
    }
    

    root_node = dalloc(PAGE_SIZE);
    root_refer = new_refer(table_name, 0, 1);
    memcpy(root_node, root_refer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) {
        db_log(PANIC, "Write table meta info error and errno %d.\n", errno);
        return false;
    } 
    
    /* Free memory. */
    dfree(root_node);
    dfree(root_refer);

    /* Close desription. */
    close(descr);

    return true;
}

static Refer *GetRootRefer(void *root_node) {
    Refer *root_refer = instance(Refer);
    memcpy(root_refer, root_node, sizeof(Refer));
    return root_node;
}


/* Calc the use rows num. */
static Size CalcUseRowsNum(Size input_size) {
    Size useSize;
    int offset;

    useSize = input_size / STRING_ROW_SIZE ;
    offset = input_size % STRING_ROW_SIZE;

    if (offset > 0)
        useSize++;

    return useSize;
}

static inline void *GetNextFreePoint(void *root_node, Refer *root_refer) {
    return ((char *) root_node) + (root_refer->page_num * PAGE_SIZE + root_refer->cell_num * STRING_ROW_SIZE);
}

static void UpdateRootRefer(Refer *root_refer, Size input_size) {

}


/* Insert new String value. 
 * Return the Refer value.
 * */
Refer *InsertStringValue(char *table_name, char *str_val) {
    Size input_size;
    Buffer buffer;
    void *root;
    Refer *root_refer, *refer;
    
    input_size = strlen(str_val) + 1;
    buffer = ReadBufferInner(table_name, STRING_TABLE_ROOT_PAGE);
    root = GetBufferPage(buffer);
    root_refer = GetRootRefer(root);
    refer = instance(Refer);
    memcpy(refer, root_refer, sizeof(Refer));
        
    /* Save the input string value. */
    memcpy(GetNextFreePoint(root, root_refer), str_val, input_size);

    dfree(root_refer);

    return refer;
}



