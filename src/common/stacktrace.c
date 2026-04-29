#include <signal.h>
#include <execinfo.h>
#include <signal.h>
#include <string.h>
#include <regex.h>
#include "data.h"
#include "defs.h"
#include "stacktrace.h"
#include "log.h"
#include "utils.h"
#include "mmgr.h"

static char *extract_addr(char *addr) {
    const char *pattern = "(0x[A-Fa-z0-9]+)";
    regex_t regex;
    int ret;

    ret = regcomp(&regex, pattern, REG_EXTENDED);
    if (ret != 0) {
        perror("Regex compile error.");
        exit(EXIT_FAILURE);
    }

    regmatch_t matchs[3];

    const char *ptr = addr;
    while (1) {
        ret = regexec(&regex, ptr, 3, matchs, 0);
        if (ret == REG_NOMATCH)
            break;

        if (ret != 0) {
            perror("Regext exec error.");
            exit(EXIT_FAILURE);
        }

        /* The first sub expr. */
        int start = matchs[1].rm_so;
        int end = matchs[1].rm_eo;
        int len = end - start;
        char *found = (char *) dalloc(len + 1);
        memcpy(found, ptr + end - len, len);
        found[len] = '\0';

        return found;
    }

    return NULL;
}

/* Print stacktrace with addr2line. 
 * Return 1 if success, otherwise return 0.
 * */
static int print_stacktrace_addr2line(char *addr) {
	char addr2line_cmd[512] = {0};

    if (!StartWith(addr, program_name))
        return 0;

    char *realAddr = extract_addr(addr);
    if (realAddr == NULL)
        return 0;

    sprintf(addr2line_cmd,"addr2line -f -e %.256s %s", program_name, realAddr); 

	FILE *fp;
	char funcName[1035];
	char lineName[1035];

	/* Open the command for reading. */
	fp = popen(addr2line_cmd, "r");
	if (fp == NULL)
		return 0;
    
	while (fgets(funcName, sizeof(funcName)-1, fp) != NULL) {
		/* If we have a pair of lines */
		if(fgets(lineName, sizeof(lineName)-1, fp) != NULL) {
			if(lineName[0] != '?') {
				/* only let func name in funcName */
				int i;
				for(i = 0; i < 1035; ++i) {
					if(funcName[i] == '\r' || funcName[i] == '\n') {
						funcName[i] = '\0';
						break;
					}
				}

				int lastSlashPos=0;
				for (i = 0; i < 1035; ++i) {
					if(lineName[i] == '\0')
						break;
					if(lineName[i] == '/')
						lastSlashPos = i + 1;
                    if (lineName[i] == '\n') {
                        lineName[i] = '\0';
                        break;
                    }
				}

                /* Record in log. */
				logger(SYS_ERROR, "%s in <%s> at %s.", addr, funcName, lineName + lastSlashPos);
			} else {
				pclose(fp);
				return 0;
			}
		} else {
			pclose(fp);
			return 0;
		}
	}

	/* close */
	pclose(fp);
    return 1;
}

/* Log the stacktrace. */
void print_stacktrace() {
    void *array[20];
    size_t size = backtrace(array, 20);
    char **strings = backtrace_symbols(array, size);
	if(strings == NULL) {
		logger(PANIC, "backtrace_symbols");
		exit(EXIT_FAILURE);
	}

    for (int i = 0; i < size; i++) {
        /* Firstly use addr2line print stacktrace. 
         * If fail, print raw stacktrace. */
        if (print_stacktrace_addr2line(strings[i]) == 0)
            logger(SYS_ERROR, "%s", strings[i]);
    }
    free(strings); 
}

static void handler(int sig) {
	switch(sig) {
		case SIGABRT:
			logger(SYS_ERROR, "Caught SIGABRT: usually caused by an abort() or assert().");
			break;
		case SIGFPE:
			logger(SYS_ERROR, "Caught SIGFPE: arithmetic exception, such as divide by zero.");
			break;
		case SIGILL:
			logger(SYS_ERROR, "Caught SIGILL: illegal instruction.");
			break;
		case SIGINT:
			logger(SYS_ERROR, "Caught SIGINT: interactive attention signal, probably a ctrl+c.");
            return;
		case SIGSEGV:
			logger(SYS_ERROR, "Caught SIGSEGV: segfault.");
			break;
		case SIGTERM:
		default:
			logger(SYS_ERROR, "Caught SIGTERM: a termination request was sent to the program.");
			break;
	}
    print_stacktrace();
}

/* Set signal handler. */
void set_signal_handler() {
	signal(SIGABRT, handler);
	signal(SIGFPE,  handler);
	signal(SIGILL,  handler);
	signal(SIGINT,  handler);
	signal(SIGSEGV, handler);
	signal(SIGTERM, handler);
	signal(SIGKILL, handler);
}


