#include "sysTable.h"


/* Create the sys table 
 * ---------------------
 * Called by db startup. 
 * Skip if already exists, otherwiese, create the sys table. 
 * */
void CreateSysTable() {

}


/* RefIdFindObject
 * ------------------
 * The interface which find object by refId.
 * Panic if not found refId.
 * */
ObjectEntity RefIdFindObject(Oid refId) {
    ObjectEntity entity;

    return entity;
}
