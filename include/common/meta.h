#include "data.h"
#include <stdint.h>

#define GET_METATABLE_NAME(meta_table) \
    meta_table->table_name

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

/* Get Really value. */
void *get_real_value(void *value, DataType type);

/* Calculate the length of table row. */
uint32_t TableCalcRowLength(Table *table);

/* Calculate the length of table row. */
uint32_t MetaTableCalcRowLenght(MetaTable *meta_table);

/* Calculate primary key lenght. if not exist primary key , return -1; */
uint32_t TableCalcPrimaryKeyLength(Table *table);

/* Calculate primary index value length. */
uint32_t TableCalcIndexLength(Table *table);

/* Genrate table meta info. */
MetaTable *GenerateMetaTable(Oid oid);

/* Find MetaColumn by column name. */
MetaColumn *NameFindMetaColumnInner(List *meta_columns, char *column_name);

/* Find MetaColumn by table name and column name. */
MetaColumn *TableColumnNameFindMetaColumn(List *meta_columns, char *table_name, char *column_name);

/* Get meta column info by column name. */
MetaColumn *NameFindMetaColumn(MetaTable *meta_table, char *name);

/* Get meta columnn postion by column name. */
int NameFindMetaColumnPostion(MetaTable *meta_table, char *column_name);

/* Get all meta column info by column name including system reserved column. */
MetaColumn *NameFindAllMetaColumn(MetaTable *meta_table, char *name);

/* Get meta column of primary key. */
MetaColumn *MetaTableFindPrimaryKey(MetaTable *meta_table);

/* Check if table exists the column. */
bool ColumnExistsInTable(char *column_name, char *table_name);

/* Calculate User level meta column length.
 * Notice, T_STRING data has added on extra char. */
uint32_t CalcUserMetaColumnLen(MetaColumn *meta_column);

/* Check if user has defined primary key.*/
bool UserPrimaryKeyExists(MetaTable *meta_table);
