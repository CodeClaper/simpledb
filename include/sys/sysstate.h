typedef enum SysState{
    SYS_NONE    = 0,
    SYS_INITED  = 1,
    SYS_READY   = 2,
    SYS_RUNNING = 3,
    SYS_PANIC   = 4
} SysState;

extern SysState sys_state;
#define SYS_IS_INITED   (sys_state >= SYS_INITED)
#define SYS_IS_READY    (sys_state >= SYS_READY)
#define SYS_IS_RUNNING  (sys_state >= SYS_RUNNING)

void MakeSysState(SysState _sys_state);

