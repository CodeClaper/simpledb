#include <stdbool.h>
#include <stdint.h>
#include "utils.h"

#ifndef SYS_H
#define SYS_H

/* Define Oid. 
 * Oid is the global unique identifier for Object 
 * like table, view, index, schema etc.
 * */
typedef uint64_t Oid;

/* OID_ZERO. 
 * OID_ZERO means the Oid not found.
 * */
#define OID_ZERO 0

#define ZERO_OID(oid) (oid == OID_ZERO)
#define NON_ZERO_OID(oid) (oid != OID_ZERO)

/* Define Rid. 
 * Rid is the global relative identifier for tuple.
 * */
typedef int64_t Rid;

/* RID_ZERO. 
 * RID_ZERO means the Rid not found.
 * */
#define RID_ZERO 0

#define ZERO_RID(rid) (rid == RID_ZERO)
#define NON_ZERO_RID(rid) (rid != RID_ZERO)

/* Define Sid. 
 * Sid is the global unique identifier for tuple.
 * */
typedef int64_t Sid;

/* SID_ZERO. 
 * SID_ZERO means the Sid not found.
 * */
#define SID_ZERO 0

#define ZERO_SID(sid) (sid == SID_ZERO)
#define NON_ZERO_SID(sid) (sid != SID_ZERO)

/* Max object relname length. */
#define MAX_RELNAME_LEN 30

/* Object Type. 
 * Only support four object type:
 * normal table, view, index table, schema, heap table, string heap table.
 * */
typedef enum ObjectType {
    OTABLE,
    OVIEW,
    OINDEX,
    OSCHEMA,
    OSID_TABLE,
    ORID_TABLE,
    OHEAP_TABLE,
    OSTRING_HEAP_TABLE
} ObjectType;

/* Object type is table or view. */
#define TABLE_OR_VIEW(type) \
        (type == OTABLE || type == OVIEW)

static char *ObjectTypeNameList[] = {
    "TABLE",
    "VIEW",
    "INDEX",
    "SCHEMA",
    "SID_TABLE",
    "RID_TABLE",
    "HEAP_TABLE",
    "STRING_HEAP_TABLE"
};

static inline char *GetObjectTypeName(ObjectType type) {
    return ObjectTypeNameList[type];
}

/* Object Entity.
 * The entity include all what an Object need.
 * */
typedef struct Object {
    Oid oid;                            /* Oid. */
    Oid toid;                           /* Table oid. */
    char relname[MAX_RELNAME_LEN];      /* Relation name. */
    ObjectType reltype;                 /* Relation type. */
} Object;


/* If table is system reserved. */
static inline bool if_table_reserved(char *table_name) {
    return false;
}

#endif 
