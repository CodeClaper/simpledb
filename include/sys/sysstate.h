typedef enum SysState{
    SYS_READY,
    SYS_RUNNING,
    SYS_PANIC
} SysState;

extern SysState sys_state;

void MakeSysState(SysState _sys_state);

#define SYS_IS_READY (sys_state == SYS_READY)
#define SYS_IS_RUNNING (sys_state == SYS_RUNNING)
