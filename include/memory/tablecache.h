#include <stdbool.h>
#include "data.h"

/* Initialise table cache. */
void InitTableCache();

/* Get all table cache. */
List *GetAllTableCache();

/* Save table cache. */
void SaveTableCache(Table *table);

/* find out if exists table in caceh. */
bool TableExistsInCache(Oid oid);

/* Find cache table by name, retrurn null if not exist. */
Table *FindTableCache(Oid oid);

/* Find table cahce by table name. */
Table *NameFindTableCache(char *tableName);

/* Remove table cache. */
void RemoveTableCache(Oid oid);
