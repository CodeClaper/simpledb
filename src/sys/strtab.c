#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "data.h"
#include "strtab.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"
#include "refer.h"
#include "bufmgr.h"

/* Create table string table. */
void CreateStrTable(char *table_name) {
    int descr;
    char str_table_name[MAX_TABLE_NAME_LEN];
    char str_table_file[MAX_TABLE_NAME_LEN + 10];
    sprintf(str_table_name, "%s_%s", table_name, STRING_TABLE_APPENDIX);
    sprintf(str_table_file, "%s_%s", str_table_name, ".dbs");
    
    AssertFalse(table_file_exist(str_table_file));
    descr = open(str_table_file, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) 
        db_log(PANIC, "Open database file '%s' fail.\n", str_table_file);
    

    void *root_node = dalloc(PAGE_SIZE);
    Refer *root_refer = new_refer(str_table_name, 0, 1);
    memcpy(root_node, root_refer, sizeof(Refer));
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    ssize_t w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) 
        db_log(PANIC, "Write table meta info error and errno %d.\n", errno);
    
    /* Close desription. */
    close(descr);
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



