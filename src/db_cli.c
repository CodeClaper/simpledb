#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <pthread.h>
#include <stdbool.h>

#define MAX_BUFF_SIZE 1<<20
#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 4080
#define BUFF_SIZE 65535
typedef unsigned char *byte_pointer;

int re_try;

/* Check if empty string. */
static bool is_empty(char *s) {
    if (s == NULL) return true;
    size_t size = strlen(s);
    if (size == 0)
        return true;
    for (int i = 0; i < size; i++) {
        if (*(s + i) != ' ')
            return false;
    }
    return true;
}

/* Execute meta stament. */
static bool meta_statment(char *input) {
    if (strcmp("clear", input) == 0 || strcmp("cls", input) == 0) {
        #ifdef _WIN32
            system("cls");
        #else
            system("clear");
        #endif
        return true;
    } else if (strcmp("history", input) == 0) {
        HIST_ENTRY** hist_entry = history_list();
        if (hist_entry == NULL) {
            fprintf(stdout, "Empty.\n");
            return true;
        }
        HIST_ENTRY *hist;
        for (uint32_t i = 0; (hist = *(hist_entry + i)) != NULL; i++ ) {
            fprintf(stdout, "%d\t%s\n", (i + 1), hist->line);
        }
        return true;
    }
    return false;
}

/* Genrate address. */
static struct sockaddr_in *gen_address(int argc, char *argv[]) {
    int opt;
    char *optString = "h:p:";
    struct sockaddr_in *address = malloc(sizeof(struct sockaddr_in));
    address->sin_family = PF_INET;
    while((opt = getopt(argc, argv, optString)) != -1) {
        switch(opt) {
            case 'h': {
                if (optarg)
                    address->sin_addr.s_addr = inet_addr(optarg);
                else 
                    address->sin_addr.s_addr = inet_addr(DEFAULT_HOST);
                break;
            }
            case 'p': {
                if (optarg)
                    address->sin_port = htons(atoi(optarg));
                else 
                    address->sin_port = htons(DEFAULT_PORT);
                break;
            }
            default: 
                fprintf(stderr, "Unrecognised parameter\n");
                exit(1);
                
        }
    }
    return address;
}

/* Try to connect. */
int try_connect(struct sockaddr_in *address) {
    int sock_fd;
    sock_fd = socket(PF_INET, SOCK_STREAM, 0);
    if (sock_fd == -1) {
        fprintf(stderr, "Create socket fail.\n");
        return -1;
    }
    if (connect(sock_fd, (struct sockaddr *)address, sizeof(*address)) == -1) {
        fprintf(stderr, "Connet server fail.\n");
        return -1;
    }

    return sock_fd;
}

/* re-connect. */
int re_connect(struct sockaddr_in *address) {

    printf("Simpledb server disconnect and try to re-connect ...\n");

    re_try++;
    if (re_try > 3) {
        printf("Retry times already exceed the maximum limitation. \n");
        exit(1);
    }
    int sock_fd = try_connect(address);

    if (sock_fd < 0)
        return re_connect(address);
    
    printf("Re-connect simpledb successfully.\n");

    return sock_fd;
}

void show_bytes(byte_pointer start, size_t len) {
    size_t i;
    for (i = 0; i <len; i++)
        printf(" %.2x", start[i]);
    printf("\n");
}

/**
 * Protocol symbol
 * GET: get the server message
 * OVER: end of session.
 */
bool db_receive(int server_fd) {
    while(true) {
        ssize_t r;
        char buff[BUFF_SIZE + 1];
        memset(buff, 0, BUFF_SIZE + 1);
        if ((r = recv(server_fd, buff, BUFF_SIZE, 0)) > 0) {
            buff[r] = '\0';
            if (strcmp(buff, "OVER") == 0) 
                return true;
            else
                printf("%s", buff);
            bzero(buff, BUFF_SIZE + 1);
        } else {
            printf("Fetch nothing\n");
            return false;
        }
    }
}


/* main
 * Command line parameters:
 * -h host
 * -p port
 * */
int main(int argc, char* argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Bad command line arguement.");
        exit(1);
    }
    struct sockaddr_in *address = gen_address(argc, argv);
    
    int sock_fd = try_connect(address);

    if (sock_fd < 0) {
        fprintf(stderr, "Connect to simpledb server fail.\n");
        exit(1);
    }

    re_try = 0;
    while(true) {
        char *input = readline("simpledb > ");
        if (is_empty(input))
            continue;
        if (strcmp("exit", input) == 0) {
            printf("Goodbye.\n");
            break;
        }
        if(meta_statment(input)) {
            add_history(input);
            free(input);
            continue;
        }
        char buff[BUFF_SIZE];
        bzero(buff, BUFF_SIZE);
        sprintf(buff, "%s", input);
        size_t result = send(sock_fd, buff, strlen(buff), 0);
        if (result == -1) {
            fprintf(stderr, "Send fail.");
            exit(1);
        }
        if (!db_receive(sock_fd)) {
            sock_fd = re_connect(address);
        }
        add_history(buff);
        free(input);
    }
    close(sock_fd);
    return 0;
}
