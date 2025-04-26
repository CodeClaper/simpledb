#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include "systable.h"
#include "mmgr.h"
#include "data.h"
#include "const.h"
#include "defs.h"
#include "table.h"
#include "log.h"
#include "timer.h"
#include "queue.h"
#include "instance.h"
#include "insert.h"
#include "select.h"
#include "delete.h"

static List *ObjCache;

/* System table meta column list. */
MetaColumn SYS_TABLE_COLUMNS[] = {
    { SYS_TABLE_OID_NAME, T_LONG, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), true, false, false, false, 0, 0},
    { SYS_TABLE_RELNAME_NAME, T_VARCHAR, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + MAX_COLUMN_SIZE), false, false, false, false, 0, 0},
    { SYS_TABLE_RELTYPE_NAME, T_INT, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), false, false, false, false, 0, 0}
};

/* System reserved columns. */
MetaColumn SYS_RESERVED_COLUMNS[] = {
    { SYS_RESERVED_ID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { CREATED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { EXPIRED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 }
}; 


static Object RowConvertObject(Row *row);

/* Find next Oid. */
inline Oid FindNextOid() {
    return (Oid) get_current_sys_time(NANOSECOND);
}

/* If system table file already exists, 
 * return true. 
 * */
static bool SysTableFileExists() {
    char sys_table_file[100];
    struct stat buf;

    memset(sys_table_file, 0, 100);
    sprintf(sys_table_file, "%s%d", conf->data_dir, SYS_ROOT_OID);

    return (stat(sys_table_file, &buf) == 0);
}

/* Create the system meta table. */
static MetaTable *CreateSysMetaTable() {
    int i;
    MetaTable *meta_table;

    meta_table = instance(MetaTable);
    meta_table->table_name = dstrdup(SYS_TABLE_NAME);
    meta_table->column_size = SYS_TABLE_COLUMNS_LENGTH;
    meta_table->all_column_size = SYS_TABLE_COLUMNS_LENGTH + SYS_RESERVED_COLUMNS_LENGTH;
    meta_table->meta_column = dalloc(sizeof(MetaColumn *) * meta_table->all_column_size);
    
    /* Define system table columns. */
    for (i = 0; i < SYS_TABLE_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, SYS_TABLE_COLUMNS + i, sizeof(MetaColumn));
        meta_table->meta_column[i] = meta_column;
    }

    /* Define system reserved columns. */
    for (; i < SYS_TABLE_COLUMNS_LENGTH + SYS_RESERVED_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, (SYS_RESERVED_COLUMNS + i - SYS_TABLE_COLUMNS_LENGTH), sizeof(MetaColumn));
        meta_table->meta_column[i] = meta_column;
    }

    return meta_table;
}

/* Create the sys table 
 * ---------------------
 * Called by db startup. 
 * Skip if already exists, otherwiese, create the sys table. 
 * Panic if fail.
 * */
void CreateSysTable() {

    /* Avoid repeat create system table. */
    if (SysTableFileExists())
        return;

    MetaTable *sysMetaTable = CreateSysMetaTable();
    if (!create_table(SYS_ROOT_OID, sysMetaTable))
        panic("Create system table fail");

    ObjCache = create_list(NODE_VOID);
}

/* Find object in cache by relname. 
 * -------------------------------
 * Return the found object or null of missing.
 * */
static Object *RelNameTypeFindObjectInCache(char *relname, ObjectType reltype) {
    ListCell *lc;
    foreach (lc, ObjCache) {
        Object *entity = (Object *)lfirst(lc);
        if (streq(relname, entity->relname) && reltype == entity->reltype)
            return entity;
    }
    return NULL;
}

/* Find object in cache by relname. 
 * -------------------------------
 * Return the found object or null of missing.
 * */
static Object *OidFindObjectInCache(Oid oid) {
    ListCell *lc;
    foreach (lc, ObjCache) {
        Object *entity = (Object *)lfirst(lc);
        if (oid == entity->oid)
            return entity;
    }
    return NULL;
}

/* Save Object in cache. */
static void SaveObjectInCache(Object object) {
    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    Object *entity = instance(Object);
    memcpy(entity, &object, sizeof(Object));
    append_list(ObjCache, entity);

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}

/* Remove object in cache. */
static void RemoveObjectInCache(Oid oid) {
    Object *entity = OidFindObjectInCache(oid);
    if (entity != NULL)
        list_delete(ObjCache, entity);
}


/* Convert Oid to a condition.
 * -------------------------
 * Generate a condition which filtered by the oid.
 * */
static ConditionNode *OidConvertCondition(Oid oid) {
    ConditionNode *condition = instance(ConditionNode);
    condition->conn_type = C_NONE;
    condition->left = NULL;
    condition->right = NULL;
    condition->predicate = instance(PredicateNode);
    condition->predicate->type = PRE_COMPARISON;
    condition->predicate->comparison = instance(ComparisonNode);
    condition->predicate->comparison->type = O_EQ;
    condition->predicate->comparison->column = instance(ColumnNode);
    condition->predicate->comparison->column->column_name = dstrdup(SYS_TABLE_OID_NAME);
    condition->predicate->comparison->value = instance(ScalarExpNode);
    condition->predicate->comparison->value->type = SCALAR_VALUE;
    condition->predicate->comparison->value->value = instance(ValueItemNode);
    condition->predicate->comparison->value->value->type = V_ATOM;
    condition->predicate->comparison->value->value->value.atom = instance(AtomNode);
    condition->predicate->comparison->value->value->value.atom->type = A_INT;
    condition->predicate->comparison->value->value->value.atom->value.intval = oid;
    return condition;
}


/* Convert relname and reltype to a condition.
 * -------------------------
 * Generate a condition which filtered by relname and reltype.
 * */
static ConditionNode *RelnameTypeConvertCondition(char *relname, ObjectType type) {
    ConditionNode *condition = instance(ConditionNode);
    condition->conn_type = C_AND;
    condition->predicate = NULL;
    condition->left = instance(ConditionNode);
    condition->right = instance(ConditionNode);

    condition->left->conn_type = C_NONE;
    condition->left->predicate = instance(PredicateNode);
    condition->left->predicate->type = PRE_COMPARISON;
    condition->left->predicate->comparison = instance(ComparisonNode);
    condition->left->predicate->comparison->type = O_EQ;
    condition->left->predicate->comparison->column = instance(ColumnNode);
    condition->left->predicate->comparison->column->column_name = dstrdup(SYS_TABLE_RELNAME_NAME);
    condition->left->predicate->comparison->value = instance(ScalarExpNode);
    condition->left->predicate->comparison->value->type = SCALAR_VALUE;
    condition->left->predicate->comparison->value->value = instance(ValueItemNode);
    condition->left->predicate->comparison->value->value->type = V_ATOM;
    condition->left->predicate->comparison->value->value->value.atom = instance(AtomNode);
    condition->left->predicate->comparison->value->value->value.atom->type = A_STRING;
    condition->left->predicate->comparison->value->value->value.atom->value.strval = relname;

    condition->right->conn_type = C_NONE;
    condition->right->predicate = instance(PredicateNode);
    condition->right->predicate->type = PRE_COMPARISON;
    condition->right->predicate->comparison = instance(ComparisonNode);
    condition->right->predicate->comparison->type = O_EQ;
    condition->right->predicate->comparison->column = instance(ColumnNode);
    condition->right->predicate->comparison->column->column_name = dstrdup(SYS_TABLE_RELTYPE_NAME);
    condition->right->predicate->comparison->value = instance(ScalarExpNode);
    condition->right->predicate->comparison->value->type = SCALAR_VALUE;
    condition->right->predicate->comparison->value->value = instance(ValueItemNode);
    condition->right->predicate->comparison->value->value->type = V_ATOM;
    condition->right->predicate->comparison->value->value->value.atom = instance(AtomNode);
    condition->right->predicate->comparison->value->value->value.atom->type = A_INT;
    condition->right->predicate->comparison->value->value->value.atom->value.intval = type;
    return condition;
}

/* Find Object by oid
 * ------------------
 * The interface which find object by oid.
 * Panic if not found or found more than one.
 * */
static Object OidFindObjectInnerInDish(Oid oid) {
    Row *row;
    ConditionNode *condition;
    SelectResult *result;

    condition = OidConvertCondition(oid);
    result = new_select_result(SELECT_STMT, NULL);
    
    /* Query. */
    query_with_condition_inner(
        SYS_ROOT_OID, condition, result, 
        select_row, ARG_NULL, NULL
    );

    /* Logically, we will get one row data. */
    if (result->row_size == 0)
        db_log(PANIC, "Not found oid %ld in system table.", oid);
    if (result->row_size > 1)
        db_log(PANIC, "Logic error, found more than one object by oid %ld in system table.", oid);
    
    row = (Row *) qfirst(QueueHead(result->rows));

    return RowConvertObject(row);
}


/* Find Object by oid
 * ------------------
 * The interface which find object by oid.
 * Panic if not found or found more than one.
 * */
static Object OidFindObjectInner(Oid oid) {
    Object entity;
    Object *found = OidFindObjectInCache(oid);
    if (found) {
        memcpy(&entity, found, sizeof(Object));
        return entity;
    }
    entity = OidFindObjectInnerInDish(oid);
    SaveObjectInCache(entity);
    return entity;
}

/* Find Object by oid */
Object OidFindObject(Oid oid) {
    Object entity;

    if (IS_SYS_ROOT(oid)) {
        entity.oid = oid;
        memcpy(entity.relname, SYS_TABLE_NAME, strlen(SYS_TABLE_NAME));
        entity.reltype = OTABLE;
    } else 
        entity = OidFindObjectInner(oid);
   
    return entity;
}

/* Find refId by relname and reltype. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
static Oid RelnameAndReltypeFindOidInDisk(char *relname, ObjectType reltype) {
    Object entity;
    ConditionNode *condition;
    SelectResult *result;
    Row *row;

    condition = RelnameTypeConvertCondition(relname, reltype);
    result = new_select_result(SELECT_STMT, NULL);

    /* Query. */
    query_with_condition_inner(
        SYS_ROOT_OID, condition, result, 
        select_row, ARG_NULL, NULL
    );

    /* The rows number maybe zero, which means the table not exists. 
     * But rows number can`t be more than one. */
    if (result->row_size == 0)
        return OID_ZERO;
    if (result->row_size > 1)
        db_log(PANIC,
               "Logic error, found more than one object by relname '%s' and reltype '%d' in system table.", 
               relname, reltype);
    
    row = (Row *) qfirst(QueueHead(result->rows));

    entity = RowConvertObject(row);

    /* Save in cache. */
    SaveObjectInCache(entity);

    return entity.oid;
}

/* Find refId by relname and reltype. 
 * ------------------------
 * Firstly, found in cache, 
 * if missing, found in disk,
 * if missing, return OID_ZERO.
 * */
static Oid RelnameAndReltypeFindOid(char *relname, ObjectType reltype) {
    Object *entity = RelNameTypeFindObjectInCache(relname, reltype);
    if (entity != NULL) 
        return entity->oid;
    return RelnameAndReltypeFindOidInDisk(relname, reltype);
}

/* Find oid of normal table by table name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid TableNameFindOid(char *tableName) {
    if (streq(tableName, SYS_TABLE_NAME))
        return SYS_ROOT_OID;
    return RelnameAndReltypeFindOid(tableName, OTABLE);
}

/* Find oid of string table by table name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid StrTableNameFindOid(char *tableName) {
    return RelnameAndReltypeFindOid(tableName, OSTRING_HEAP_TABLE);
}

/* Find relname by oid. 
 * ---------------------
 * Return relname which need free by caller.
 * */
char *OidFindRelName(Oid oid) {
    AssertFalse(ZERO_OID(oid));
    Object entity = OidFindObject(oid);
    return dstrdup(entity.relname);
}

/* Convert rows to object list. */
static List *RowsConvertObjectList(Queue *qRow) {
    List *list = create_list(NODE_VOID);
    
    QueueCell *qc;
    qforeach (qc, qRow) {
        Row *row = (Row *) qfirst(qc);
        Object entity = RowConvertObject(row);
        Object *datum = instance(Object);
        memcpy(datum, &entity, sizeof(Object));
        append_list(list, datum);
    }

    return list;
}

/* Find all object list. */
List *FindAllObject() {

    SelectResult *result = new_select_result(SELECT_STMT, NULL);
    
    /* Query. */
    query_with_condition_inner(
        SYS_ROOT_OID, NULL, result, 
        select_row, ARG_NULL, NULL
    );
    
    return RowsConvertObjectList(result->rows);
}

/* Geneate Object entity. 
 * The oid supported by caller.
 * */
Object GenerateObjectInner(Oid oid, char *relname, ObjectType reltype) {
    Size len;
    Object entity;

    len = strlen(relname) + 1;
    Assert(MAX_RELNAME_LEN >= len);

    entity.oid = oid;
    entity.reltype = reltype;
    memset(entity.relname, 0, MAX_RELNAME_LEN);
    memcpy(entity.relname, relname, len);
    
    return entity;
}

/* Geneate Object entity. 
 * The oid is geneate by FindNextOid.
 * */
Object GenerateObject(char *relname, ObjectType reltype) {
    return GenerateObjectInner(FindNextOid(), relname, reltype);
}

/* Convert KeyValue. */
static void *ObjectConvertKeyValue(Object entity, int i) {
    switch (i) {
        case 0: {
            Oid *oid = instance(Oid);
            *oid = entity.oid;
            return oid;
        }
        case 1: 
            return dstrdup(entity.relname);
        case 2: {
            ObjectType *type = instance(ObjectType);
            *type = entity.reltype;
            return type;
        }
        default:
            panic("Logic error, overflow the Object fields scope.");
    }
    return NULL;
}

/* Convert row to object. */
static Object RowConvertObject(Row *row) {
    Object entity;

    char *relname = ((KeyValue *)lfirst(list_nth_cell(row->data, 1)))->value;
    ObjectType *type = ((KeyValue *)lfirst(list_nth_cell(row->data, 2)))->value;

    entity.oid = *(Oid *)row->key;
    memset(entity.relname, 0, MAX_RELNAME_LEN);
    memcpy(entity.relname, relname, MAX_RELNAME_LEN);
    entity.reltype = *type;

    return entity;
}

/* Convert Object value to row. 
 * Return a new row which need be freed by caller.
 * */
static Row *ObjectConvertRow(Object entity) {
    Row *row = instance(Row);

    row->key = instance(Oid);
    *(Oid *)(row->key) = entity.oid;
    memcpy(row->table_name, SYS_TABLE_NAME, strlen(SYS_TABLE_NAME));
    row->data = create_list(NODE_KEY_VALUE);

    int i;
    for (i = 0; i < SYS_TABLE_COLUMNS_LENGTH; i++) {
        MetaColumn meta_column = SYS_TABLE_COLUMNS[i];
        KeyValue *key_value = new_key_value(
            dstrdup(meta_column.column_name),
            ObjectConvertKeyValue(entity, i),
            meta_column.column_type
        );
        append_list(row->data, key_value);
    }
    
    /* Make up the reserved columns. */
    supple_reserved_column(row);

    return row;
}

/* Save Object. */
bool SaveObject(Object entity) {
    Row *row;
    Refer *refer;
    Table *sysTable;

    row = ObjectConvertRow(entity);
    sysTable = open_table_inner(SYS_ROOT_OID); 
    Assert(sysTable);
    refer = insert_one_row(sysTable, row);

    dfree(row);
    return refer != NULL;
}

/* Remove the object. */
bool RemoveObject(Oid oid) {
    SelectResult *result;
    ConditionNode *condition;

    result = new_select_result(DELETE_STMT, NULL);
    condition = OidConvertCondition(oid);
    
    /* Query. */
    query_with_condition_inner(
        SYS_ROOT_OID, condition, result, 
        delete_row, ARG_NULL, NULL
    );
    
    /* Remove cache object. */
    RemoveObjectInCache(oid);

    return result->row_size > 0;
}
