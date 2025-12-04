#include "data.h"

#ifndef XLOG_H
#define XLOG_H

/* XLog Heap Type. */
typedef enum XLogHeapType {
    HEAP_INSERT,
    HEAP_DELETE,
    HEAP_UPDATE_INSERT,
    HEAP_UPDATE_DELETE
} XLogHeapType;

/* XLogEntry */
typedef struct XLogEntry {
    Xid xid;                    /* Transaction Id */
    Oid oid;                    /* Oid. */
    Sid sid;                    /* Sid. */
    XLogHeapType type;          /* XLog Head type. */
    struct XLogEntry *next;     /* Next XLogEntry */
} XLogEntry;

/* Record Xlog. */
void RecordXlog(Oid oid, Sid sid, XLogHeapType type);

/* Commit Xlog. */
void CommitXlog();

/* Execute rollback. */
void ExecuteRollback();

#endif
