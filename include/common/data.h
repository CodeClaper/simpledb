#include <bits/types/struct_timeval.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include "c.h"
#include "list.h"
#include "queue.h"

#ifndef DATA_H
#define DATA_H

#define PAGE_SIZE 16384
#define ACTUAL_PAGE_SIZE 8192

#define MAX_TABLE_PAGE 10000000
#define MAX_COLUMN_SIZE 256     // max column size
#define MAX_COLUMN_NAME_LEN 30  // max column name length
#define MAX_MULTI_TABLE_NUM 30  // max multi-table number.
#define BUFF_SIZE 1024
#define SPOOL_SIZE 65535        /* Spool buffer size. */

#define MAX_COLUMN_NAME_LEN 30  // max column name length
#define MAX_TABLE_NAME_LEN 30

#define MAX_INT_VALUE   (1<<31) - 1
#define MAX_UINT_VALUE  (1<<32) - 1
#define MAX_LONG_VALUE  (1L<<63) - 1

#define MENTRY_STYPE_LENGTH 48

#define ARRAY_FLARE_FACTOR 10

#define SYMBOL_LENGTH 11
#define MAX_BOOL_STR_LENGTH 10
#define MAX_INT_STR_LENGTH 20
#define MAX_LONG_STR_LENGTH 30
#define MAX_FLOAT_STR_LENGTH 50
#define MAX_DOUBLE_STR_LENGTH 100
#define MAX_DATE_STR_LENGTH 30
#define MAX_TIMESTAMP_STR_LENGTH 30

#define MAX_DEFAULT_VALUE_LENGTH 64
#define MAX_COMMENT_STRING_LENGTH 64

#define SYS_RESERVED_ID_COLUMN_NAME  "sys_id"
#define CREATED_XID_COLUMN_NAME  "created_xid"
#define EXPIRED_XID_COLUMN_NAME  "expired_xid"

/* System Data type. */

/* CompareType */
typedef enum { O_EQ, O_NE, O_GT, O_GE, O_LT, O_LE } CompareType;

/* DataType */
typedef enum DataType {T_UNKNOWN, T_BOOL, T_CHAR, T_VARCHAR, T_INT, T_LONG, T_DOUBLE, T_FLOAT, T_STRING, T_DATE, T_TIMESTAMP, T_REFERENCE, T_ROW } DataType;

/* FunctionType */
typedef enum { F_COUNT, F_MAX, F_MIN, F_SUM, F_AVG } FunctionType;

/* ConnType */
typedef enum { C_OR, C_AND, C_NONE} ConnType; // connector type

/* FunctionValueType */
typedef enum { V_INT, V_COLUMN, V_ALL } FunctionValueType; // value type.

/* SelectItemType */
typedef enum { SELECT_COLUMNS, SELECT_FUNCTION, SELECT_ALL } SelectItemType;

/* ConditionNodeType */
typedef enum { LOGIC_CONDITION, EXEC_CONDITION } ConditionNodeType;

/* ShowNodeType */
typedef enum { SHOW_TABLES  } ShowNodeType;

/* StatementType */
typedef enum { 
    UNKONWN_STMT,
    LOGIN_STMT,
    BEGIN_TRANSACTION_STMT, 
    COMMIT_TRANSACTION_STMT, 
    ROLLBACK_TRANSACTION_STMT, 
    CREATE_TABLE_STMT, 
    SELECT_STMT, 
    INSERT_STMT, 
    UPDATE_STMT, 
    DELETE_STMT, 
    DESCRIBE_STMT, 
    SHOW_STMT, 
    DROP_TABLE_STMT,
    ALTER_TABLE_STMT,
} StatementType; // statement type

/* Tansaction operation type. */
typedef enum { TR_SELECT, TR_INSERT, TR_DELETE, TR_UPDATE } TransOpType;

/* NodeState. */
typedef enum { INUSE_STATE, OBSOLETE_STATE, DIRTY_STATE } NodeState;

/* NodeType */
typedef enum { UNKNOWN_NODE, LEAF_NODE, INTERNAL_NODE } NodeType;

/* ExecuteResult */
typedef enum { 
    EXECUTE_SUCCESS = 200, 
    EXECUTE_FAIL, 
    EXECUTE_SQL_ERROR, 
    EXECUTE_TABLE_NOT_EXIST_FAIL, 
    EXECUTE_TABLE_CREATE_FAIL, 
    EXECUTE_TABLE_DROP_FAIL, 
    EXECUTE_TABLE_OPEN_FAIL,
    EXECUTE_DUPLICATE_COLUMN,
    EXECUTE_NOT_MATCH_COLUMN,
    EXECUTE_UNKNOWN_COLUMN,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_EXCEEDED_MAX_COLUMN,
    EXECUTE_OPEN_DATABASE_FAIL,
    EXECUTE_RW_DATABASE_FAIL,
    EXECUTE_CONVERT_DATA_TYPE_FAIL
} ExecuteStatus;

/* LogLevel */
typedef enum { 
    TRACE,      /* Show detail infomation. */ 
    DEBUGER,    /* Show infomation to help debuger,*/
    INFO,       /* DB running Infomation. */
    SUCCESS,    /* Success result to client. */
    WARN,       /* For unexpected messages including sql syntaxt error, reapeated begin transaction or commit .etc. */ 
    ERROR,      /* User error, will abort transaction. */
    SYS_ERROR,  /* System error */
    FATAL,      /* Abort process. */
    PANIC       /* Shut down the database. */
} 
LogLevel;

/* Lock level. */
typedef enum { LEVEL_ROW, LEVEL_TABLE } LockLevel;

/* Lock mode. */
typedef enum LockMode { RD_MODE, WR_MODE } LockMode;

/* The Four Transaction Isolation Level. */
typedef enum { READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE } TransIsolationLevel;


/* DataTypeNode */
typedef struct DataTypeNode {
    DataType type;
    uint32_t len;
    char *table_name;
} DataTypeNode;

/* ColumnNode */
typedef struct ColumnNode {
    char *column_name;
    char *range_variable;
    bool has_sub_column;
    struct ColumnNode *sub_column;
    List *scalar_exp_list;
} ColumnNode;

/* FunctionValueType */
typedef struct FunctionValueNode {
    FunctionValueType value_type;
    union {
        int32_t i_value;
        ColumnNode *column;
    };
} FunctionValueNode;

/* FunctionNode */
typedef struct FunctionNode {
    FunctionType type;
    FunctionValueNode *value;
} FunctionNode;

/* CalculateType */
typedef enum CalculateType {
    CAL_ADD,
    CAL_SUB,
    CAL_MUL,
    CAL_DIV
} CalculateType;

/* CalculateNode */
typedef struct CalculateNode {
    CalculateType type;
    struct ScalarExpNode *left;
    struct ScalarExpNode *right;
} CalculateNode;

/* SelectItemsNode */
typedef struct {
    List *column_list;
    FunctionNode *function_node;
    SelectItemType type;
} SelectItemsNode;

/* SelectionNode */
typedef struct SelectionNode {
    bool all_column;
    List *scalar_exp_list;
} SelectionNode;

/* ScalarExpType */
typedef enum ScalarExpType {
    SCALAR_CALCULATE,
    SCALAR_COLUMN,
    SCALAR_FUNCTION,
    SCALAR_VALUE
} ScalarExpType;

/* ScalarExpNode */
typedef struct ScalarExpNode {
    ScalarExpType type;
    union {
        CalculateNode *calculate;
        ColumnNode *column;
        FunctionNode *function;
        struct ValueItemNode *value;
    };
    char *alias;
} ScalarExpNode;

typedef enum BaseTableElementType {
    TELE_COLUMN_DEF,
    TELE_TABLE_CONTRAINT_DEF
} BaseTableElementType;

/* BaseTableElementNode. */
typedef struct BaseTableElementNode {
    BaseTableElementType type;
    struct ColumnDefNode *column_def;
    struct TableContraintDefNode *table_contraint_def;
} BaseTableElementNode; 

/* ColumnDefOptType */
typedef enum ColumnDefOptType {
    OPT_NOT_NULL,
    OPT_UNIQUE,
    OPT_PRIMARY_KEY,
    OPT_DEFAULT_VALUE,
    OPT_DEFAULT_NULL,
    OPT_COMMENT,
    OPT_CHECK_CONDITION,
    OPT_REFERENECS
} ColumnDefOptType;

/* ColumnDefOptNode */
typedef struct ColumnDefOptNode {
    ColumnDefOptType opt_type;
    struct ValueItemNode *value;
    struct ConditionNode *condition;
    char *refer_table;
    char *comment;
} ColumnDefOptNode;

/* TableContraintType */
typedef enum TableContraintType {
    TCONTRAINT_UNIQUE,
    TCONTRAINT_PRIMARY_KEY,
    TCONTRAINT_FOREIGN_KEY,
    TCONTRAINT_CHECK
} TableContraintType;

/* TableContraintDefNode */
typedef struct TableContraintDefNode {
    TableContraintType type;
    List *column_commalist;
    char *table;
    struct ConditionNode *condition;
} TableContraintDefNode;

/* ColumnDefName */
typedef struct ColumnDefName {
    char *column;
} ColumnDefName;

/* ColumnDefNode */
typedef struct ColumnDefNode { 
    ColumnDefName *column;         /* Column defination name. */
    DataTypeNode *data_type;       /* Column defination data type. */ 
    uint32_t array_dim;            /* Array dimension, default zero if not arrary. */ 
    List *column_def_opt_list;     /* Column defination operation list. */
} ColumnDefNode;

/* PrimaryKeyNode */
typedef struct {
    ColumnNode *column;
} PrimaryKeyNode;

/* ValuesOrQuerySpecType */
typedef enum ValuesOrQuerySpecType {
    VQ_VALUES,
    VQ_QUERY_SPEC
} ValuesOrQuerySpecType;

/* ValuesOrQuerySpecNode. */
typedef struct {
    ValuesOrQuerySpecType type;         /* Type*/
    List *values;                       /* List of Value List. */
    struct QuerySpecNode *query_spec;   /* QuerySpecNode. */
} ValuesOrQuerySpecNode;

/* ReferFetchType */
typedef enum ReferFetchType {
    DIRECTLY = 1,
    INDIRECTLY
} ReferFetchType;

/* ReferValue */
typedef struct ReferValue {
    ReferFetchType type;
    union {
        /* For directly. */
        List *nest_value_list;
        /* For indirectly. */
        struct ConditionNode *condition;
    };
} ReferValue;

/* ArrayValue. */
typedef struct ArrayValue {
    DataType type;
    List *list;
} ArrayValue;

/* AtomType. */
typedef enum AtomType {
    A_INT,
    A_BOOL,
    A_STRING,
    A_FLOAT,
    A_REFERENCE
} AtomType;

/* AtomNode. */
typedef struct AtomNode {
    AtomType type;
    union {
        int64_t intval;
        bool boolval;
        char *strval;
        double floatval;
        ReferValue *referval;
    } value;
} AtomNode;

/* ValueItemType. */
typedef enum ValueItemType {
    V_ATOM,
    V_NULL,
    V_ARRAY
} ValueItemType;

/* ValueItemNode. */
typedef struct ValueItemNode {
    ValueItemType type;
    union {
        AtomNode *atom;
        List *value_list;
    } value;
} ValueItemNode;


/* QuerySpecNode. */
typedef struct QuerySpecNode {
    struct SelectionNode *selection;
    struct TableExpNode *table_exp;
} QuerySpecNode;

/* AssignmentNode */
typedef struct {
    ColumnNode *column;
    ValueItemNode *value;
} AssignmentNode;

/* ConditionNode */
typedef struct ConditionNode {
    ConnType conn_type;
    struct ConditionNode *left;
    struct ConditionNode *right;
    struct PredicateNode *predicate;
} ConditionNode;

/* PredicateType */
typedef enum PredicateType {
    PRE_COMPARISON,
    PRE_LIKE,
    PRE_IN
} PredicateType;

/* PredicateNode */
typedef struct PredicateNode {
    PredicateType type;
    union {
        struct ComparisonNode *comparison;
        struct LikeNode *like;
        struct InNode *in;
    };
} PredicateNode;

/* ComparisonNode */
typedef struct ComparisonNode {
    CompareType type;
    ColumnNode *column;
    ScalarExpNode *value;
} ComparisonNode;

/* LikeNode */
typedef struct LikeNode {
    ColumnNode *column;
    ValueItemNode *value;
} LikeNode;

/* InNode */
typedef struct InNode {
    ColumnNode *column;
    List *value_list;
} InNode;

/* LimitClauseNode */
typedef struct LimitClauseNode {
    int32_t offset;
    int32_t rows;
} LimitClauseNode;

/* TableRefNode. */
typedef struct TableRefNode {
    char *table;
    char *range_variable;
} TableRefNode;

/* FromClauseNode. */
typedef struct FromClauseNode {
    List *from;
} FromClauseNode;

/* WhereClauseNode. */
typedef struct WhereClauseNode {
    ConditionNode *condition; 
} WhereClauseNode;

/* TableExpNode */
typedef struct TableExpNode {
    FromClauseNode *from_clause;
    WhereClauseNode *where_clause;
    LimitClauseNode *limit_clause;
} TableExpNode;

/* CreateTableNode */
typedef struct {
    char *table_name;
    List *base_table_element_commalist;
} CreateTableNode;

/* DropTableNode */
typedef struct DropTableNode {
    char *table_name;
} DropTableNode;

/* SelectNode */
typedef struct SelectNode {
    SelectionNode *selection;
    TableExpNode *table_exp;
} SelectNode;

/* InsertNode */
typedef struct InsertNode {
    bool all_column;
    char *table_name;
    List *column_list;
    ValuesOrQuerySpecNode *values_or_query_spec;
} InsertNode;

/* UpdateNode */
typedef struct UpdateNode {
    char *table_name;
    List *assignment_list;
    WhereClauseNode *where_clause;
} UpdateNode;

/* DeleteNode */
typedef struct DeleteNode {
    char *table_name;
    ConditionNode *condition_node;
} DeleteNode;

/* DescribeNode */
typedef struct DescribeNode {
    char *table_name;
} DescribeNode;

/* ShowNode */
typedef struct ShowNode {
    ShowNodeType type;
} ShowNode;

/* AlterTableActionType */
typedef enum AlterTableActionType {
    ALTER_TO_ADD_COLUMN,
    ALTER_TO_DROP_COLUMN
} AlterTableActionType;

/* PositionType */
typedef enum PositionType {
    POS_BEFORE,
    POS_AFTER
} PositionType;

/* ColumnPositionDef */
typedef struct ColumnPositionDef {
    PositionType type;
    char *column;
} ColumnPositionDef;

/* AddColumnDef */
typedef struct AddColumnDef {
    ColumnDefNode *column_def;
    ColumnPositionDef *position_def;
} AddColumnDef;

/* DropColumnDef. */
typedef struct DropColumnDef {
    char *column_name;
} DropColumnDef;


/* AlterTableAction */
typedef struct AlterTableAction {
    AlterTableActionType type;
    union {
        AddColumnDef *add_column;
        DropColumnDef *drop_column;
    } action;
} AlterTableAction;


/* AlterTableNode */
typedef struct AlterTableNode {
    char *table_name;
    AlterTableAction *action;
} AlterTableNode;

/* Statement */
typedef struct Statement {
  StatementType statement_type;
  union {
        CreateTableNode *create_table_node;
        DropTableNode *drop_table_node;
        SelectNode *select_node;
        InsertNode *insert_node;
        UpdateNode *update_node;
        DeleteNode *delete_node;
        DescribeNode *describe_node;
        ShowNode *show_node;
        AlterTableNode *alter_table_node;
  };
} Statement;

/* Pager */
typedef struct Pager {
    char *table_name;
    uint32_t file_length;
    volatile uint32_t size;
    List *pages;
    List *buffers;
} Pager;

typedef enum DefaultValueType {
    DEFAULT_VALUE_NONE = 0,
    DEFAULT_VALUE_NULL,
    DEFAULT_VALUE
} DefaultValueType;

/* MetaColumn */
typedef struct MetaColumn {
    char column_name[MAX_COLUMN_NAME_LEN];      /* Column Name. */
    DataType column_type;                       /* Column data type. */
    char table_name[MAX_TABLE_NAME_LEN];        /* Owned table Name */
    uint32_t column_length;                     /* Column data length. Not allowed exceed the length limit. */
    bool is_primary;                            /* Primary-key column. */
    bool not_null;                              /* Not-null column. */
    bool is_unique;                             /* Unique column. */
    bool sys_reserved;                          /* System reserved column, only visible for system. */
    uint32_t array_dim;                         /* Array dimension. Default zero if not array. */
    uint32_t array_cap;                         /* Array capacity. (array_cap = array_dim * n) */
    DefaultValueType default_value_type;        /* Default value type. */
    void *default_value;                        /* Default value. */
    bool has_comment;                           /* Has comment. */
    char comment[MAX_COMMENT_STRING_LENGTH];    /* Comment */
} MetaColumn;

/* MetaTable */
typedef struct MetaTable {
    char *table_name;
    MetaColumn **meta_column;
    uint32_t column_size;       /* size of column, excluding system reserved columns. */
    uint32_t all_column_size;   /* sizo of column, including system reserved columns. */
} MetaTable;

/* Table */
typedef struct Table {
    uint32_t root_page_num;     /* Root page num. */
    MetaTable *meta_table;      /* Meta table info. */
    Pid creator;                /* The creator pid. */
    Size page_size;             /* Page size. */
} Table;

/* TableBufferEntry */
typedef struct TableBufferEntry {
    Table *table;
    int64_t xid;
} TableBufferEntry;

/* Cursor */
typedef struct Cursor {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
} Cursor;

/* KeyValue */
typedef struct KeyValue {
    char *key;
    void *value;
    DataType data_type;
    char *table_name;
    bool is_array;
} KeyValue;

/* Row */
typedef struct Row {
    void *key;                              /* Primary key. */
    char table_name[MAX_TABLE_NAME_LEN];    /* Table name. */
    List *data;                             /* List of KeyValue. */
} Row;

/* SelectResult */
typedef struct SelectResult {
    StatementType stype;            /* Statement type. */
    char *table_name;               /* Table name. */
    char *range_variable;           /* Range variable. */
    uint32_t row_size;              /* Row size. Although in rows list indicates the row size, these row_size works for count agg. */
    Queue *rows;                    /* Selected rows. */
    struct SelectResult *derived;   /* Derived select result, used for multi-table query. */
    bool last_derived;              /* Last derived flag. */
} SelectResult;


/* MEntry */
typedef struct MEntry {
    void *ptr;
    size_t size;
    char stype[MENTRY_STYPE_LENGTH];
    struct MEntry *next;
} MEntry;

/* MHashTable */
typedef struct {
    MEntry **entry_list;
    uint32_t num;                           /* number of entry list. */
    uint32_t capacity;                      /* capacity of table cell. */
} MHashTable;


/* Abount configuration. */
typedef struct {
    char *data_dir;                         /* Database file directory. */
    ushort port;                            /* Server listening port. */
    char *log_dir;                          /* Log directory */
    LogLevel log_level;                     /* Log level */
    TransIsolationLevel trans_iso_level;    /* Transaction Isolation level.*/
    bool auto_rollback;                     /* If auto rollback. */
    char *account;                          /* Account. */
    char *password;                         /* password. */
} Conf;

/* Refer */
typedef struct Refer {
    char table_name[MAX_TABLE_NAME_LEN];
    int32_t page_num;
    int32_t cell_num;
} Refer;

/* ReferUpdateEntity */
typedef struct ReferUpdateEntity {
    Refer *old_refer;
    Refer *new_refer;
} ReferUpdateEntity;

/* Db execute result. */
typedef struct {
    StatementType stmt_type;
    char *table;
    bool success;
    void *data;
    uint32_t rows;
    char *message;
    struct timeval start_time;
    struct timeval end_time;
    double duration;
} DBResult;

/* TransactionHandle */
typedef struct TransEntry {
    Xid xid;                    /* transaction id. */ 
    Pid pid;                    /* processor id. */
    bool auto_commit;           /* auto commit. */
    struct TransEntry *next;    /* next */
} TransEntry;


/* AliasEntry */
typedef struct AliasEntry {
    char *name;
    char *alias;
} AliasEntry;

/* AliasMap */
typedef struct AliasMap {
    uint32_t size;
    AliasEntry map[MAX_MULTI_TABLE_NUM];
} AliasMap;

/* InternalNodeCellEntry. */
typedef struct InternalNodeCellEntry {
    void *key;
    DataType key_type;
    uint32_t value;
} InternalNodeCellEntry;


#endif
