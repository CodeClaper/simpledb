#include <sched.h>
#include <stdint.h>

#define INT64CONST(x)  (x##L)
#define UINT64CONST(x) (x##UL)

#define forever for(;;)

/* Size 
 * Size of any memory resident object.
 */
typedef size_t Size;

/* Pid
 * The processor Id.
 * */
typedef pid_t Pid;

/* Xid
 * The transaction Id.
 * */
typedef int64_t Xid;

/*
 * Index
 */
typedef uint32_t Index;
