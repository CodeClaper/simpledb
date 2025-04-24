#include "data.h"

typedef struct TableRegEntry {
    Oid oid;
    pid_t pid;
    struct TableRegEntry *next;
} TableRegEntry;


/* Init tablereg. */
void init_table_reg();


/* Try to register TableRegEntry, 
 * if already exists one, not actually call register_table_reg() */
void try_register_table_reg(Oid oid);


/* Destroy TableRegEntry by pid. */
void destroy_table_reg();


/* Check if table shared by other pid. */
bool if_shared_table(Oid oid);
