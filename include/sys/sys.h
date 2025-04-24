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

#define ZERO_OID(oid) oid == OID_ZERO

/* Max object relname length. */
#define MAX_RELNAME_LEN 30

/* Object Type. 
 * Only support four object type:
 * normal table, view, index table and schema.
 * */
typedef enum ObjectType {
    OTABLE,
    OVIEW,
    OINDEX,
    OSCHEMA,
    OSTRING_HEAP_TABLE
} ObjectType;

static char *ObjectTypeNameList[] = {
    "TABLE",
    "VIEW",
    "INDEX",
    "SCHEMA",
    "STRING_HEAP_TABLE"
};

static inline char *GetObjectTypeName(ObjectType type) {
    return ObjectTypeNameList[type];
}

/* Object Entity.
 * The entity include all what an Object need.
 * */
typedef struct Object {
    Oid oid;
    char relname[MAX_RELNAME_LEN];
    ObjectType reltype;
} Object;


/* If table is system reserved. */
static inline bool if_table_reserved(char *table_name) {
    return false;
}

#endif 
