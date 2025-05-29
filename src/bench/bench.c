#include <signal.h>
#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "socket.h"
#include "dbsocket.h"

#define VERSION "V1.0.0"

static char *optString = "h:p:r:t:c:?:V";
static const struct option long_options[]=
{
    {"host",no_argument,NULL, 'h'},
    {"port",no_argument,NULL, 'p'},
    {"run",no_argument,NULL, 'r'},
    {"time",required_argument,NULL,'t'},
    {"clients",required_argument,NULL,'c'},
    {"help",no_argument,NULL,'?'},
    {"version",no_argument,NULL,'V'},
    {NULL,0,NULL,0}
};

int mypipe[2];
int timerexpired = 0;

static char *host = "127.0.0.1";
static int port = 4083;
static int clients = 1;
static int time = 30;
static char *sql = "";

/* Usage print out. */
static void usage() {
    fprintf(stderr,
            "bench [option]...\n"
            "  -h|--host                Database host. Default 127.0.0.1.\n"
            "  -p|--port <n>            Database port. Default 4083.\n"
            "  -r|--run                 Run sql.\n"
            "  -t|--time <sec>          Run for <sec> seconds. Default 30.\n"
            "  -c|--clients <n>         Run <n> clients at once. Default one.\n"
            "  -?|--help                This information.\n"
            "  -V|--version             Display program version.\n"
           );
    exit(0);
}

/* Version print out. */
static void version() {
    printf("%s\n", VERSION);
    exit(0);
}

/* Get opteration. */
static void getOpt(int argc, char *argv[]) {
    int opt=0;
    int options_index=0;
    while ((opt = getopt_long(argc, argv, optString, long_options, &options_index)) != EOF) {
        switch (opt) {
            case 'h': {
                host = optarg;
                break;
            }
            case 'p': {
                port = atoi(optarg);
                break;
            }
            case 'r': {
                sql = optarg;
                break;
            }
            case 't': {
                time = atoi(optarg);
                break;
            }
            case 'c': {
                clients = atoi(optarg);
                break;
            }
            case '?': {
                usage();
                break;
            }
            case 'V': {
                version();
                break;
            }
        }
    }
}

/* Info show. */
static void showInfo() {
    fprintf(stdout,
            "Run info:\n"
            "  -Host:%s\n"
            "  -Port:%d\n"
            "  -Run:%s\n"
            "  -time:%d\n"
            "  -Clients:%d\n"
           , host, port, sql, time, clients);
}

static void alarmHandler(int signal) {
    timerexpired = 1;
}	

static void benchCore() {
    struct sigaction sa;
    /* setup alarm signal handler */
    sa.sa_handler = alarmHandler;
    sa.sa_flags=0;
    if(sigaction(SIGALRM, &sa, NULL))
        exit(3);
    alarm(time); // after benchtime,then exit

    while (1) {
        /* Exist when timer expired. */
        if (timerexpired) {
            return;
        }

    }
}


/* Bench */
static void bench() {
    int i, client;
    pid_t pid=0;

    /* Check socket avaliable. */
    client = Socket(host, port);
    if (client < 0) { 
        fprintf(stderr,"Connect to database server failed. Aborting benchmark.\n");
        exit(1);
    }
    close(client);
    
    /* Create pipe */
    if(pipe(mypipe)) {
        fprintf(stderr,"Create pip fail.\n");
        exit(3);
    }
    
    for (i = 0; i < clients; i++) {
        pid=fork();
        if(pid <= (pid_t) 0) 
        {
            /* child process or error*/
            sleep(1); /* make childs faster */
            break;
        }
    }

    /* Fork child process. */
    if (pid < (pid_t) 0) {
        fprintf(stderr,"Fork child process fail.\n");
        exit(3);
    } else if (pid == (pid_t) 0) {
        benchCore();
    } else {

    }
}



/* The main entry. */
int main(int argc, char *argv[]) {

    if (argc == 1) {
        usage();
        return 2;
    }
    
    getOpt(argc, argv);
    if (sql == NULL || strcmp(sql, "") == 0) {
        fprintf(stderr, "Must support run sql\n");
        usage();
    }

    showInfo();

    tryConnect(host, port, "root", "Zc120130211");
}
