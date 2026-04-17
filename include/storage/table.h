#include "data.h"
#include "defs.h"
#include <stdbool.h>


/* Get table name. */
#define GET_TABLE_NAME(table)\
    table->meta_table->table_name

/* Get table oid. */
#define GET_TABLE_OID(table)\
    table->oid

bool check_table_exist_direct(Oid oid);
bool check_table_exist(char *table_name); 
bool check_index_exist(char *index_name);
bool table_file_exist(char *table_file_path);
char *table_file_path(Oid refId);
List *LoadMetaIndex(Oid toid, Table *table);
Table *load_table(Oid oid);
Table *open_table_inner(Oid oid);
Table *open_table(char *table_name);
bool create_table(Oid oid, MetaTable *meta_table);
bool ShrinkTable(Table *table);
bool drop_table(char *table_name);


