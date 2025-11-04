#include "data.h"
#include <stdint.h>

#define GET_METATABLE_NAME(meta_table) \
    meta_table->table_name

/* Function type names*/
static char *FUNCTION_TYPE_NAMES[] = { "COUNT", "MAX", "MIN", "SUM", "AVG" };
static inline char *GET_FUNCTION_TYPE_NAME(FunctionType function_type) {
   return FUNCTION_TYPE_NAMES[function_type];
}

/* DataTypeNames */
static char *DATA_TYPE_NAMES[] =  { "unknown", "bool",  "char", "varchar", "int", "long", "double", "float", "string", "date", "timestamp",  "reference", "object" };
static inline char *GET_DATA_TYPE_NAME(DataType data_type) {
    return DATA_TYPE_NAMES[data_type];
}

/* Default data length. */
uint32_t DataTypeDefaultLength(DataType column_type);

/* Convert AtomType to DataType. */
DataType AtomTypeConvertDataType(AtomType atom_type);

/* Assign value from ValueItemNode. */
void *ValueItemNodeAssignValue(ValueItemNode *value_item_node, MetaColumn *meta_column);

/* Get value from value item node. */
void *ValueItemNodeFindValue(ValueItemNode *value_item_node);

/* Get Comparable value. */
void *GetComparableValue(void *value, DataType type);

/* Get key string value for system.*/
char *KeyGetUserStrValue(void *key, DataType ptype);

/* Get key string value for user.*/
char *KeyGetSysStrValue(void *key, DataType ptype);

/* Calculate the length of table row. */
uint32_t TableCalcRowLength(Table *table);

/* Calculate the length of table row. */
uint32_t MetaTableCalcRowLenght(MetaTable *meta_table);

/* Calculate primary key lenght. if not exist primary key , return -1; */
uint32_t TableCalcPrimaryKeyLength(Table *table);

/* Calculate primary index value length. */
uint32_t TableCalcIndexLength(Table *table);

/* Get key type name. */
char *GetKeyTypeName(MetaColumn *meta_column);

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

/* Get meta column data type. */
DataType MetaTableFindPrimaryDataType(MetaTable *meta_table);

/* Check if table exists the column. */
bool ColumnExistsInTable(char *column_name, char *table_name);

/* Calculate User level meta column length.
 * Notice, T_STRING data has added on extra char. */
uint32_t CalcUserMetaColumnLen(MetaColumn *meta_column);

/* Check if user has defined primary key.*/
bool UserPrimaryKeyExists(MetaTable *meta_table);

/* Seriable meta column. */
void *MetaColumnSeriable(MetaColumn *meta_column);

/* Deseriable meta column. */
MetaColumn *MetaColumnDeseriable(void *destination);

/* Assign value to destination. */
void MetaColumnAssignValueToDestination(void *destination, void *value, MetaColumn *meta_column);
