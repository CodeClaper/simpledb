#include <stdbool.h>
#include "data.h"

/* Initialise table cache. */
void InitTableCache();

/* Save table cache. */
void SaveTableCache(Table *table);

/* find out if exists table in caceh. */
bool TableExistsInCache(Oid oid);

/* Find cache table by name, retrurn null if not exist. */
Table *FindTableCache(Oid oid);

/* Remove table cache. */
void RemoveTableCache(Oid oid);
