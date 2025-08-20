/********************************* Auth Module ************************************
 * Auth:        JerryZhou
 * Created:     2023/09/25
 * Modify:      2024/09/25
 * Locataion:   src/backend/auth.c
 * Description: Auth modeule is intended to check login info. 
 **********************************************************************************/
#include <stdbool.h>
#include <stdio.h>
#include "auth.h"
#include "defs.h"
#include "utils.h"

/* Auth the login message. */
bool auth(char *message) {
    char buf[BUFF_SIZE];
    sprintf(buf, "%s/%s", conf->account, conf->password);
    return StrEq(buf, message);
}

