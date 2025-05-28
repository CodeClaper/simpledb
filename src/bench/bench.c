#include <stdio.h>

/* Usage print out. */
static void usage() {
    fprintf(stderr,
            "bench [option]...\n"
            "  -t|--time <sec>          Run benchmark for <sec> seconds. Default 30.\n"
            "  -c|--clients <n>         Run <n> HTTP clients at once. Default one.\n"
            "  -?|-h|--help             This information.\n"
            "  -V|--version             Display program version.\n"
           );
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        usage();
        return 2;
    }
}
