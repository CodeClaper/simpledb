#include "data.h"
#include "refer.h"

/* Btree search the key refer. */
Refer *BtreeSearchRefer(Oid oid, void *key);

/* Btree search for value. */
void *BtreeSearchValue(Oid oid, void *key);

/* Btree seach key via refer.*/
void *BtreeSearchKeyViaRefer(Refer *refer);

/* Btree seach value via refer.*/
void *BtreeSearchValueViaRefer(Refer *refer);

/* Btree search key via ref id. */
void *BtreeSearchKeyViaRefId(Oid oid, Rid ref_id);

/* Btree search value via ref id. */
void *BtreeSearchValueViaRefId(Oid oid, Rid ref_id);
