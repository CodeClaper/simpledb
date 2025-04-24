#include "data.h"
#include "defs.h"
#include <stdbool.h>


/* Get table name. */
#define GET_TABLE_NAME(table)\
    table->meta_table->table_name

/* Get table oid. */
#define GET_TABLE_OID(table)\
    table->oid

/* Get table list. */
List *get_table_list();

/* Check if table exist directly. */
bool check_table_exist_direct(Oid oid);

/* Check if table exists. */
bool check_table_exist(char *table_name); 


/* Check table file if exist 
 * Return true if exist or false if not exist. */
bool table_file_exist(char *table_file_path);


/* Get table file path. */
char *table_file_path(Oid refId);


/* Open a table object. */
Table *open_table_inner(Oid oid);


/* Open a table file. */
Table *open_table(char *table_name);


/* Create a new table. */
bool create_table(Oid oid, MetaTable *meta_table);


/* Add new MetaColumn to table.
 * This function is actually bottom-level routine for alter-table-add-column action.
 * */
bool add_new_meta_column(char *table_name, MetaColumn *new_meta_column, ColumnPositionDef *pos);


/* Drop table`s meta_column. */
bool drop_meta_column(char *tbale_name, char *column_name);


/* Change table`s meta_table. */
bool change_meta_column(char *table_name, char *column_name, MetaColumn *new_meta_column);


/*Delete an existed table. */
bool drop_table(char *table_name);


