#include "data.h"

/* Btree search the key refer. */
Refer *BtreeSearchRefer(Oid oid, void *key);

/* Btree search for value. */
void *BtreeSearchValue(Oid oid, void *key);

/* Btree seach key via refer.*/
void *BtreeSearchKeyViaRefer(Refer *refer);

/* Btree seach value via refer.*/
void *BtreeSearchValueViaRefer(Refer *refer);
