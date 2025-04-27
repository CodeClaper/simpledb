#include "sysstate.h"

SysState sys_state;

/* Change the machin state. */
inline void MakeSysState(SysState _sys_state) {
    sys_state = _sys_state;
}

