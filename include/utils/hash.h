#include "c.h"
#include "sys.h"

/* HashCode. */
typedef unsigned long Hash;

/* Hash a String value. */
Hash StringHash(char *strVal, Size size);

/* Has an Oid value. */
Hash OidHash(Oid oid, Size size);
