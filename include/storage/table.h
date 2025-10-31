#include "data.h"
#include "defs.h"
#include <stdbool.h>


/* Get table name. */
#define GET_TABLE_NAME(table)\
    table->meta_table->table_name

/* Get table oid. */
#define GET_TABLE_OID(table)\
    table->oid

/* Check if table exist directly. */
bool check_table_exist_direct(Oid oid);

/* Check if table exists. */
bool check_table_exist(char *table_name); 

/* Check if index exists. */
bool check_index_exist(char *index_name);

/* Check table file if exist 
 * Return true if exist or false if not exist. */
bool table_file_exist(char *table_file_path);

/* Get table file path. */
char *table_file_path(Oid refId);

/* Load Table from disk. */
Table *load_table(Oid oid);

/* Open a table object. */
Table *open_table_inner(Oid oid);

/* Open a table file. */
Table *open_table(char *table_name);

/* Create a new table. */
bool create_table(Oid oid, MetaTable *meta_table);

/* Create a new table. */
bool shrink_table(Oid oid, MetaTable *meta_table);

/*Delete an existed table. */
bool drop_table(char *table_name);


