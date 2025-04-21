#include <stdbool.h>
#include "strheaptable.h"
#include "utils.h"

#ifndef SYS_H
#define SYS_H

/* Define Oid. 
 * Oid is the global unique identifier for Object 
 * like table, view, index, schema etc.
 * */
typedef unsigned int Oid;

/* Object Type. 
 * Only support four object type:
 * normal table, view, index table and schema.
 * */
typedef enum ObjectType {
    TABLE,
    VIEW,
    INDEX,
    SCHEMA
} ObjectType;

/* Object Entity.
 * The entity include all what an Object need.
 * */
typedef struct ObjectEntity {
    Oid refId;
    char relName[MAX_TABLE_NAME_LEN];
    ObjectType relType;
} ObjectEntity;

/* If table is system reserved. */
static inline bool if_table_reserved(char *table_name) {
    return false;
}

#endif 
