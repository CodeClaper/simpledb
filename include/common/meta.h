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
static char *DATA_TYPE_NAMES[] =  { "unknown", "bool",  "char", "varchar", "int", "long", "double", "float", "string", "date", "timestamp",  "rid", "referValue", "object" };
static inline char *GET_DATA_TYPE_NAME(DataType data_type) {
    return DATA_TYPE_NAMES[data_type];
}

/* Index type name. */
static char *INDEX_TYPE_NAMES[] = { "BTREE", "HASH", "GIN" };
static inline char *GET_INDEX_TYPE_NAME(IndexType type) {
    return INDEX_TYPE_NAMES[type];
}

uint32_t DataTypeDefaultLength(DataType column_type);
DataType AtomTypeConvertDataType(AtomType atom_type);
void *ValueItemNodeAssignValue(ValueItemNode *value_item_node, MetaColumn *meta_column);
void *ValueItemNodeFindValue(ValueItemNode *value_item_node);
KeyValue *QueryTupleValueItem(ValueItemNode *value_item);
void *GetComparableValue(void *value, DataType type);
DataType GetComparableType(DataType type);
char *KeyGetUserStrValue(void *key, DataType ptype);
char *KeyGetSysStrValue(void *key, DataType ptype);
uint32_t TableCalcRowLength(Table *table);
uint32_t MetaTableCalcRowLenght(MetaTable *meta_table);
uint32_t TableCalcPrimaryKeyLength(Table *table);
uint32_t TableCalcIndexLength(Table *table);
char *GetKeyTypeName(MetaColumn *meta_column);
MetaColumn *NameFindMetaColumnInner(List *meta_columns, char *column_name);
MetaColumn *TableColumnNameFindMetaColumn(List *meta_columns, Oid toid, char *column_name);
MetaColumn *NameFindMetaColumn(MetaTable *meta_table, char *name);
int NameFindMetaColumnPostion(MetaTable *meta_table, char *column_name);
MetaColumn *NameFindAllMetaColumn(MetaTable *meta_table, char *name);
MetaColumn *MetaTableFindPrimaryKey(MetaTable *meta_table);
DataType MetaTableFindPrimaryDataType(MetaTable *meta_table);
bool ColumnExistsInTable(char *column_name, char *table_name);
uint32_t CalcUserMetaColumnLen(MetaColumn *meta_column);
bool UserPrimaryKeyExists(MetaTable *meta_table);
void *MetaColumnSeriable(MetaColumn *meta_column);
MetaColumn *MetaColumnDeseriable(void *destination);
void MetaColumnAssignValueToDestination(void *destination, void *value, MetaColumn *meta_column);
MetaIndex *TableFindPrimaryMetaIndex(Table *table);
