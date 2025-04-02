#include "data.h"
#include <stdint.h>

/* Default data length. */
uint32_t default_data_len(DataType column_type);


/* Convert AtomType to DataType. */
DataType convert_data_type(AtomType atom_type);


/* Data type name. */
char *data_type_name(DataType data_type);


/* Function type name. */
char *function_type_name(FunctionType function_type);


/* Check if system built-in primary key.*/
bool built_in_primary_key(MetaTable *meta_table);


/* Assign value from ValueItemNode. */
void *assign_value_from_value_item_node(ValueItemNode *value_item_node, MetaColumn *meta_column);


/* Get value from value item node. */
void *get_value_from_value_item_node(ValueItemNode *value_item_node, MetaColumn *meta_column);


/* Calculate the length of table row. */
uint32_t calc_table_row_length(Table *table);


/* Calculate the length of table row. */
uint32_t calc_table_row_length2(MetaTable *meta_table);


/* Calculate primary key lenght. if not exist primary key , return -1; */
uint32_t calc_primary_key_length(Table *table);


/* Calculate primary key lenght. if not exist primary key , return -1; */
uint32_t calc_primary_key_length2(MetaTable *meta_table);


/* Genrate table meta info. */
MetaTable *gen_meta_table(Table *table, char *table_name);


/* Get meta column info by column name. */
MetaColumn *get_meta_column_by_name(MetaTable *meta_table, char *name);


/* Get meta columnn postion by column name.
 * Return -1 if missing. 
 * */
int get_meta_column_pos_by_name(MetaTable *meta_table, char *column_name);


/* Get all meta column info by column name including system reserved column. */
MetaColumn *get_all_meta_column_by_name(MetaTable *meta_table, char *name);


/* Get meta column of primary key. */
MetaColumn *get_primary_key_meta_column(MetaTable *meta_table);



/* Stringify the value according to data type. */
char *stringify_value(void *value, DataType data_type);



/* Get default value name from MetaColumn. */
char *get_default_value_name(MetaColumn *meta_column);



/* Check if table exists the column. */
bool if_exists_column_in_table(char *column_name, char *table_name);


/* Calculate Raw meta column length.
 * Notice, T_STRING data has added on extra char. */
uint32_t calc_raw_meta_column_len(MetaColumn *meta_column);


/* Check if user has defined primary key.*/
bool has_user_primary_key(MetaTable *meta_table);

/* Get the created xid. */
Xid get_created_xid(void *destinct, MetaTable *meta_table);

/* Get the expired xid. */
Xid get_expired_xid(void *destinct, MetaTable *meta_table);

/* Get the created xid. */
uint32_t get_created_xid_offset(MetaTable *meta_table);

/* Get the expired xid. */
uint32_t get_expired_xid_offset(MetaTable *meta_table);
