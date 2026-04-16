#include "data.h"

typedef struct TableRegEntry {
    Oid oid;
    pid_t pid;
    struct TableRegEntry *next;
} TableRegEntry;


void init_table_reg();
void try_register_table_reg(Oid oid);
void destroy_table_reg();
bool if_shared_table(Oid oid);
