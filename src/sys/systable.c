#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include "systable.h"
#include "mmgr.h"
#include "data.h"
#include "const.h"
#include "defs.h"
#include "table.h"
#include "log.h"
#include "random.h"
#include "queue.h"
#include "instance.h"
#include "insert.h"
#include "select.h"
#include "delete.h"
#include "trans.h"
#include "meta.h"
#include "row.h"
#include "tuple.h"
#include "sysstate.h"
#include "heaptable.h"
#include "optimizer.h"

/* System table meta column list. */
MetaColumn SYS_TABLE_COLUMNS[] = {
    {SYS_ROOT_OID, SYS_TABLE_OID_NAME, T_LONG, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), 0, true, false, false, false, 0, 0},
    {SYS_ROOT_OID, SYS_TABLE_TOID_NAME, T_LONG, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), true, false, false, false, 0, 0},
    {SYS_ROOT_OID, SYS_TABLE_RELNAME_NAME, T_VARCHAR, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + MAX_COLUMN_SIZE), (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)) * 2, false, false, false, false, 0, 0}, 
    {SYS_ROOT_OID, SYS_TABLE_RELTYPE_NAME, T_INT, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), ((LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)) * 2+ LEAF_NODE_CELL_NULL_FLAG_SIZE + MAX_COLUMN_SIZE), false, false, false, false, 0, 0}
};

/* System reserved columns. */
MetaColumn SYS_RESERVED_COLUMNS[] = {
    {0, SYS_RESERVED_ID_COLUMN_NAME, T_LONG, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), 0, false, false, false, true, 0, 0},
    {0, CREATED_XID_COLUMN_NAME, T_LONG, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), 0, false, false, false, true, 0, 0},
    {0, EXPIRED_XID_COLUMN_NAME, T_LONG, 0, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), 0, false, false, false, true, 0, 0}
}; 

static Object TupleConvertObject(void *tuple);

/* Find next Oid. */
inline Oid FindNextOid() {
    return RandomUint64();
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
    meta_table->meta_columns = create_list(NODE_META_COLUMN);
    
    /* Define system table columns. */
    for (i = 0; i < SYS_TABLE_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, SYS_TABLE_COLUMNS + i, sizeof(MetaColumn));
        append_list(meta_table->meta_columns, meta_column);
    }

    /* Define system reserved columns. */
    for (; i < SYS_TABLE_COLUMNS_LENGTH + SYS_RESERVED_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, (SYS_RESERVED_COLUMNS + i - SYS_TABLE_COLUMNS_LENGTH), sizeof(MetaColumn));
        append_list(meta_table->meta_columns, meta_column);
    }

    return meta_table;
}

/* Create the sys table 
 * ---------------------
 * Called by db startup. 
 * Skip if already exists, otherwiese, create the sys table. 
 * Panic if fail.
 * */
void InitSysTable() {
    /* Avoid repeat create system table. */
    if (SysTableFileExists()) return;

    MetaTable *sysMetaTable = CreateSysMetaTable();
    if (!create_table(SYS_ROOT_OID, sysMetaTable) || !CreateHeapTableInner(SYS_ROOT_HEAP_OID))
        panic("Create system table fail");
}


/* Convert Oid to a condition.
 * -------------------------
 * Generate a condition which filtered by the oid.
 * */
static SearchConditionNode *OidConvertCondition(Oid oid) {
    SearchConditionNode *search_condition = instance(SearchConditionNode);
    BooleanTermNode *boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *predicate = instance(PredicateNode);

    /* Assemble the predicate. */
    predicate = instance(PredicateNode);
    predicate->type = PRE_COMPARISON;
    predicate->comparison = instance(ComparisonNode);
    predicate->comparison->type = O_EQ;
    predicate->comparison->left = instance(ScalarExpNode);
    predicate->comparison->left->type = SCALAR_COLUMN;
    predicate->comparison->left->column = instance(ColumnNode);
    predicate->comparison->left->column->column_name = dstrdup(SYS_TABLE_OID_NAME);
    predicate->comparison->right = instance(ScalarExpNode);
    predicate->comparison->right->type = SCALAR_VALUE;
    predicate->comparison->right->value = instance(ValueItemNode);
    predicate->comparison->right->value->type = V_ATOM;
    predicate->comparison->right->value->value.atom = instance(AtomNode);
    predicate->comparison->right->value->value.atom->type = A_INT;
    predicate->comparison->right->value->value.atom->value.intval = oid;

    /* Assemble All. */
    boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    boolean_primary->predicate = predicate;
    boolean_test->type = NONE_TRUE_VALUE;
    boolean_test->boolean_primary = boolean_primary;
    boolean_factor->is_not = false;
    boolean_factor->boolean_test = boolean_test;
    boolean_term->boolean_factor = boolean_factor;
    search_condition->boolean_term = boolean_term;

    return search_condition;
}


/* Convert relname and reltype to a condition.
 * ------------------------------------------
 * Generate a condition which filtered by relname and reltype. */
static SearchConditionNode *RelnameTypeConvertCondition(char *relname, ObjectType type) {
    SearchConditionNode *search_condition = instance(SearchConditionNode);
    BooleanTermNode *boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *predicate = instance(PredicateNode);

    BooleanTermNode *and_boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *and_boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *and_boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *and_boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *and_predicate = instance(PredicateNode);

    /* Assemble one predicate. */
    predicate = instance(PredicateNode);
    predicate->type = PRE_COMPARISON;
    predicate->comparison = instance(ComparisonNode);
    predicate->comparison->type = O_EQ;
    predicate->comparison->left = instance(ScalarExpNode);
    predicate->comparison->left->type = SCALAR_COLUMN;
    predicate->comparison->left->column = instance(ColumnNode);
    predicate->comparison->left->column->column_name = dstrdup(SYS_TABLE_RELNAME_NAME);
    predicate->comparison->right = instance(ScalarExpNode);
    predicate->comparison->right->type = SCALAR_VALUE;
    predicate->comparison->right->value = instance(ValueItemNode);
    predicate->comparison->right->value->type = V_ATOM;
    predicate->comparison->right->value->value.atom = instance(AtomNode);
    predicate->comparison->right->value->value.atom->type = A_STRING;
    predicate->comparison->right->value->value.atom->value.strval = relname;

    /* Assemble another predicate. */
    and_predicate = instance(PredicateNode);
    and_predicate->type = PRE_COMPARISON;
    and_predicate->comparison = instance(ComparisonNode);
    and_predicate->comparison->type = O_EQ;
    and_predicate->comparison->left = instance(ScalarExpNode);
    and_predicate->comparison->left->type = SCALAR_COLUMN;
    and_predicate->comparison->left->column = instance(ColumnNode);
    and_predicate->comparison->left->column->column_name = dstrdup(SYS_TABLE_RELTYPE_NAME);
    and_predicate->comparison->right = instance(ScalarExpNode);
    and_predicate->comparison->right->type = SCALAR_VALUE;
    and_predicate->comparison->right->value = instance(ValueItemNode);
    and_predicate->comparison->right->value->type = V_ATOM;
    and_predicate->comparison->right->value->value.atom = instance(AtomNode);
    and_predicate->comparison->right->value->value.atom->type = A_INT;
    and_predicate->comparison->right->value->value.atom->value.intval = type;
    
    /* Assemble All. */
    boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    boolean_primary->predicate = predicate;
    boolean_test->type = NONE_TRUE_VALUE;
    boolean_test->boolean_primary = boolean_primary;
    boolean_factor->is_not = false;
    boolean_factor->boolean_test = boolean_test;
    boolean_term->boolean_factor = boolean_factor;
    search_condition->boolean_term = boolean_term;

    and_boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    and_boolean_primary->predicate = and_predicate;
    and_boolean_test->type = NONE_TRUE_VALUE;
    and_boolean_test->boolean_primary = and_boolean_primary;
    and_boolean_factor->is_not = false;
    and_boolean_factor->boolean_test = and_boolean_test;
    and_boolean_term->boolean_factor = and_boolean_factor;

    /* Relation the and boolean term. */
    boolean_term->and_boolean_term = and_boolean_term;

    return search_condition;
}

/* Convert toid and reltype to a condition.
 * ------------------------------------------
 * Generate a condition which filtered by relname and reltype. */
static SearchConditionNode *ToidTypeConvertCondition(Oid toid, ObjectType type) {
    SearchConditionNode *search_condition = instance(SearchConditionNode);
    BooleanTermNode *boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *predicate = instance(PredicateNode);

    BooleanTermNode *and_boolean_term = instance(BooleanTermNode);
    BooleanFactorNode *and_boolean_factor = instance(BooleanFactorNode);
    BooleanTestNode *and_boolean_test = instance(BooleanTestNode);
    BooleanPrimaryNode *and_boolean_primary = instance(BooleanPrimaryNode);
    PredicateNode *and_predicate = instance(PredicateNode);

    /* Assemble one predicate. */
    predicate = instance(PredicateNode);
    predicate->type = PRE_COMPARISON;
    predicate->comparison = instance(ComparisonNode);
    predicate->comparison->type = O_EQ;
    predicate->comparison->left = instance(ScalarExpNode);
    predicate->comparison->left->type = SCALAR_COLUMN;
    predicate->comparison->left->column = instance(ColumnNode);
    predicate->comparison->left->column->column_name = dstrdup(SYS_TABLE_TOID_NAME);
    predicate->comparison->right = instance(ScalarExpNode);
    predicate->comparison->right->type = SCALAR_VALUE;
    predicate->comparison->right->value = instance(ValueItemNode);
    predicate->comparison->right->value->type = V_ATOM;
    predicate->comparison->right->value->value.atom = instance(AtomNode);
    predicate->comparison->right->value->value.atom->type = A_INT;
    predicate->comparison->right->value->value.atom->value.intval = toid;

    /* Assemble another predicate. */
    and_predicate = instance(PredicateNode);
    and_predicate->type = PRE_COMPARISON;
    and_predicate->comparison = instance(ComparisonNode);
    and_predicate->comparison->type = O_EQ;
    and_predicate->comparison->left = instance(ScalarExpNode);
    and_predicate->comparison->left->type = SCALAR_COLUMN;
    and_predicate->comparison->left->column = instance(ColumnNode);
    and_predicate->comparison->left->column->column_name = dstrdup(SYS_TABLE_RELTYPE_NAME);
    and_predicate->comparison->right = instance(ScalarExpNode);
    and_predicate->comparison->right->type = SCALAR_VALUE;
    and_predicate->comparison->right->value = instance(ValueItemNode);
    and_predicate->comparison->right->value->type = V_ATOM;
    and_predicate->comparison->right->value->value.atom = instance(AtomNode);
    and_predicate->comparison->right->value->value.atom->type = A_INT;
    and_predicate->comparison->right->value->value.atom->value.intval = type;
    
    /* Assemble All. */
    boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    boolean_primary->predicate = predicate;
    boolean_test->type = NONE_TRUE_VALUE;
    boolean_test->boolean_primary = boolean_primary;
    boolean_factor->is_not = false;
    boolean_factor->boolean_test = boolean_test;
    boolean_term->boolean_factor = boolean_factor;
    search_condition->boolean_term = boolean_term;

    and_boolean_primary->type = PREDICATE_BOOLEAN_PRIMAYR; 
    and_boolean_primary->predicate = and_predicate;
    and_boolean_test->type = NONE_TRUE_VALUE;
    and_boolean_test->boolean_primary = and_boolean_primary;
    and_boolean_factor->is_not = false;
    and_boolean_factor->boolean_test = and_boolean_test;
    and_boolean_term->boolean_factor = and_boolean_factor;

    /* Relation the and boolean term. */
    boolean_term->and_boolean_term = and_boolean_term;

    return search_condition;
}


/* Find Object by oid
 * ------------------
 * The interface which find object by oid.
 * Panic if not found or found more than one.
 * */
static Object OidFindObjectInner(Oid oid) {
    void *tuple;
    SearchConditionNode *condition;
    SelectResult *result;

    condition = OidConvertCondition(oid);
    result = new_select_result(SELECT_STMT, SYS_TABLE_NAME, true);
    
    /* Query. */
    QueryUnderSearchConditionInner(
        SYS_ROOT_OID, result, 
        SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, condition)
    );

    /* Logically, we will get one row data. */
    if (result->row_size == 0)
        db_log(PANIC, "Not found oid %ld in system table.", oid);
    if (result->row_size > 1)
        db_log(PANIC, "Logic error, found more than one object by oid %ld in system table.", oid);
    
    tuple = (void *) qfirst(QueueHead(result->tuples));

    return TupleConvertObject(tuple);
}

/* Find Object by oid */
Object OidFindObject(Oid oid) {
    Object entity;
    memset(&entity, 0, sizeof(Object));

    if (IS_SYS_ROOT(oid)) {
        entity.oid = oid;
        memcpy(entity.relname, SYS_TABLE_NAME, strlen(SYS_TABLE_NAME));
        entity.reltype = OTABLE;
    } else if (IS_SYS_ROOT_HEAP(oid)) {
        entity.oid = oid;
        memcpy(entity.relname, SYS_TABLE_NAME, strlen(SYS_TABLE_NAME));
        entity.reltype = OHEAP_TABLE;
    } else
        entity = OidFindObjectInner(oid);
   
    return entity;
}

/* Find refId by relname and reltype. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
static Oid RelnameAndReltypeFindOid(char *relname, ObjectType reltype) {
    Object entity;
    SearchConditionNode *condition;
    SelectResult *result;
    void *tuple;

    condition = RelnameTypeConvertCondition(relname, reltype);
    result = new_select_result(SELECT_STMT, SYS_TABLE_NAME, true);

    /* Query. */
    QueryUnderSearchConditionInner(
        SYS_ROOT_OID, result, 
        SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, condition)
    );

    /* The rows number maybe zero, which means the table not exists. 
     * But rows number can`t be more than one. */
    if (result->row_size == 0)
        return OID_ZERO;
    if (result->row_size > 1)
        db_log(PANIC,
               "Logic error, found more than one object by relname '%s' and reltype '%d' in system table.", 
               relname, reltype);
    
    tuple = (void *) qfirst(QueueHead(result->tuples));

    entity = TupleConvertObject(tuple);

    return entity.oid;
}

/* Find oid of normal table by table name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid TableNameFindOid(char *tableName) {
    if (StrEq(tableName, SYS_TABLE_NAME))
        return SYS_ROOT_OID;
    return RelnameAndReltypeFindOid(tableName, OTABLE);
}

/* Find oid of index table by index name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid IndexNameFindOid(char *indexName) {
    return RelnameAndReltypeFindOid(indexName, OINDEX);
}

/* Find oid of string table by table name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid StrTableNameFindOid(char *tableName) {
    if (StrEq(tableName, SYS_TABLE_NAME))
        return OID_ZERO;
    return RelnameAndReltypeFindOid(tableName, OSTRING_HEAP_TABLE);
}

/* Find oid of heap table by table name. 
 * ------------------------
 * Return the oid of the found object.
 * Return OID_ZERO if missing.
 * */
Oid TableNameFindHeapOid(char *tableName) {
    if (StrEq(tableName, SYS_TABLE_NAME))
        return SYS_ROOT_HEAP_OID;
    return RelnameAndReltypeFindOid(tableName, OHEAP_TABLE);
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

/* Find indexs by toid. 
 * --------------------
 * Return list of index oid.
 * */
List *ToidFindIndexs(Oid toid) {

    SearchConditionNode *condition;
    SelectResult *result;
    List *indexs;
    
    indexs = create_list(NODE_VOID);

    /* Systable no indexs. */
    if (IS_SYS_ROOT(toid)) return indexs;

    condition = ToidTypeConvertCondition(toid, OINDEX);
    result = new_select_result(SELECT_STMT, SYS_TABLE_NAME, true);

    /* Query. */
    QueryUnderSearchConditionInner(
        SYS_ROOT_OID, result, 
        SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, condition)
    );
    
    QueueCell *qc;
    qforeach (qc, result->tuples) {
        void *tuple = (void *) qfirst(qc);
        append_list(indexs, TupleFindValue(tuple, SYS_TABLE_COLUMNS + 0));
    }

    return indexs;
}

/* Convert tuples to object list. */
static List *TuplesConvertObjectList(Queue *qTuples) {
    List *list = create_list(NODE_VOID);
    
    QueueCell *qc;
    qforeach (qc, qTuples) {
        void *tuple = (void *) qfirst(qc);
        Object entity = TupleConvertObject(tuple);
        Object *datum = instance(Object);
        memcpy(datum, &entity, sizeof(Object));
        append_list(list, datum);
    }

    return list;
}


/* Find all object list. */
List *FindAllObject() {
    SelectResult *result;

    result = new_select_result(SELECT_STMT, SYS_TABLE_NAME, true);
    /* Query. */
    QueryUnderSearchConditionInner(
        SYS_ROOT_OID, result, 
        SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, NULL)
    );

    return TuplesConvertObjectList(result->tuples);
}

/* Geneate Object entity. 
 * The oid supported by caller.
 * */
Object GenerateObjectInner(Oid oid, Oid toid, char *relname, ObjectType reltype) {
    Size len;
    Object entity;

    len = strlen(relname) + 1;
    Assert(MAX_RELNAME_LEN >= len);

    entity.oid = oid;
    entity.toid = toid;
    entity.reltype = reltype;
    memset(entity.relname, 0, MAX_RELNAME_LEN);
    memcpy(entity.relname, relname, len);
    
    return entity;
}

/* Geneate Object entity. 
 * The oid is geneate by FindNextOid.
 * */
Object GenerateObject(Oid toid, char *relname, ObjectType reltype) {
    return GenerateObjectInner(FindNextOid(), toid, relname, reltype);
}

/* Convert KeyValue. */
static void *ObjectConvertKeyValue(Object entity, int i) {
    switch (i) {
        case 0: {
            Oid *oid = instance(Oid);
            *oid = entity.oid;
            return oid;
        }
        case 1: {
            Oid *toid = instance(Oid);
            *toid = entity.toid;
            return toid;
        }
        case 2: 
            return dstrdup(entity.relname);
        case 3: {
            ObjectType *type = instance(ObjectType);
            *type = entity.reltype;
            return type;
        }
        default:
            panic("Logic error, overflow the Object fields scope.");
    }
    return NULL;
}

/* Convert the tuple to object. */
static Object TupleConvertObject(void *tuple) {
    Object entity;

    Oid *oid = (Oid *) TupleFindValue(tuple, SYS_TABLE_COLUMNS + 0);
    Oid *toid = (Oid *) TupleFindValue(tuple, SYS_TABLE_COLUMNS + 1);
    char *relname = (char *) TupleFindValue(tuple, SYS_TABLE_COLUMNS + 2);
    ObjectType *type = (ObjectType *) TupleFindValue(tuple, SYS_TABLE_COLUMNS + 3);

    entity.oid = *oid;
    entity.toid = *toid;
    memset(entity.relname, 0, MAX_RELNAME_LEN);
    memcpy(entity.relname, relname, MAX_RELNAME_LEN);
    entity.reltype = *type;

    return entity;
}

/* Convert Object value to row. 
 * Return a new row which need be freed by caller.
 * */
static Row *ObjectConvertRow(Object entity) {
    Row *row = NewRow();

    int i;
    for (i = 0; i < SYS_TABLE_COLUMNS_LENGTH; i++) {
        MetaColumn meta_column = SYS_TABLE_COLUMNS[i];
        KeyValue *key_value = new_key_value(meta_column.column_name, 
                                            ObjectConvertKeyValue(entity, i), 
                                            meta_column.column_type, 
                                            meta_column.tid);
        append_list(row->data, key_value);
    }
    
    /* Make up the reserved columns. */
    MakeupReservedColumns(SYS_ROOT_OID , row);

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
    refer = InsertForRow(sysTable, row);

    dfree(row);
    return refer != NULL;
}

/* Remove the object. */
bool RemoveObject(Oid oid) {
    SelectResult *result;
    SearchConditionNode *condition;

    result = new_select_result(DELETE_STMT, SYS_TABLE_NAME, true);
    condition = OidConvertCondition(oid);
    
    /* Query. */
    QueryUnderSearchConditionInner(
        SYS_ROOT_OID, result, 
        SimpleSelectPlan(delete_row, ARG_NULL, NULL, condition)
    );
    
    return result->row_size > 0;
}
