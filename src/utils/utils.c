#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <ctype.h>
#include <math.h>
#include <errno.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <float.h>
#include "data.h"
#include "utils.h"
#include "mmgr.h"

/* Left trim. 
 * Notice: not use the s directly, 
 * rather than the returning vlaue. */
char *ltrim(char *s) {
    while(isspace(*s)) s++;
    return s;
}

/* Right trim. 
 * Notice: although you can use s directly, 
 * but not recommand that. */
char *rtrim(char *s) {
    char* back = s + strlen(s);
    while(isspace(*--back));
    *(back+1) = '\0';
    return s;
}

/* trim */
char *trim(char *s) {
    return rtrim(ltrim(s)); 
}

/* Check if a string contains substring.*/
bool contains(char* str, char *substr) {
    if (!str || !substr)
        return false;
    char *s = strstr(str, substr);
    return s != NULL;
}

/* Check if a file has prefix. */
bool startwith(char *str, const char *prefix) {
    if (!str || !prefix)
        return false;
    ssize_t str_len = strlen(str);
    ssize_t pre_size = strlen(prefix);
    if (pre_size > str_len)
        return false;
    return strncmp(str, prefix, pre_size) == 0;
}

/* check if a file has suffix. */
bool endwith(char *str, char *suffix) {
    if (!str || !suffix)
        return false;
    ssize_t str_len = strlen(str);
    ssize_t suffix_size = strlen(suffix);
    if (suffix_size > str_len)
        return false;
    return strcmp(str + str_len - suffix_size, suffix) == 0;
}

/* substring */
char *substr(char *str, uint32_t start, uint32_t end) {
    if (!str)
        return NULL;
    ssize_t str_size = strlen(str);
    if (start >= str_size || end >= str_size)
        return NULL;
    char *substr = dalloc(end - start + 1);
    uint index = 0;
    for (uint32_t i = 0; i <str_size; i++) {
        if (start <= i && i <= end) {
            *(substr + index) = *(str + i);
            index++;
        }
    }
    *(substr + index) = '\0';
    return substr;
}

/* replace onece */
char *replace_once(char *str, const char *old_str, const char *new_str) {
    if (!str || !old_str)
        return NULL;
    ssize_t str_size = strlen(str);
    ssize_t old_size = strlen(old_str);
    if (old_size > str_size)
        return NULL;
    if (new_str == NULL)
        new_str = "";
    ssize_t new_size = strlen(new_str);
    char *ret = dalloc(str_size - old_size + new_size + 1);

    uint32_t index;
    for (index = 0; index < str_size; index++) {
        if (strncmp(str + index, old_str, old_size) == 0) {
            memcpy(ret + index , new_str, new_size); 
            strcpy(ret + index + new_size, str + index + old_size);
            *(ret + str_size - old_size + new_size) = '\0';
            return ret;
        }
        *(ret + index) = *(str + index);
    }
    dfree(ret);
    return NULL;
}

/* Replace all. */
char *replace_all(char *str, char *old_str, char *new_str) {
    if (!str || !old_str)
        return NULL;
    ssize_t str_size = strlen(str);
    ssize_t old_size = strlen(old_str);
    if (old_size > str_size)
        return NULL;
    if (new_str == NULL)
        new_str = "";
    ssize_t new_size = strlen(new_str);
    
    ssize_t size = str_size + 1;
    char* ret = dalloc(size);
    uint32_t i, j;
    for (i = 0, j = 0; i < str_size; i++, j++) {
        if (strncmp(str + i, old_str, old_size) == 0) {
            if (j + new_size - old_size + 1 > size) {
                size = j + new_size -old_size + 1;
                ret = drealloc(ret, size);
            }
            memcpy(ret + j, new_str, new_size);
            i += old_size;
            j += new_size;
        } 
        *(ret + j) = *(str + i);
    }

    *(ret + j) = '\0';
    
    return ret;
}

/* Check if empty string. */
bool is_empty(char *s) {
    if (s == NULL) 
        return true;
    size_t size = strlen(s);
    if (size == 0)
        return true;
    for(int i = 0; i < size; i++) {
        if (*(s + i) != ' ')
            return false;
    }
    return true;
}

/* Format String and return. */
char *format(char *format, ...) {
    char message[BUFF_SIZE];
    va_list ap;
    va_start(ap, format);
    vsprintf(message, format, ap);
    char *ret = dstrdup(message);
    va_end(ap);
    return ret;
}

/* Check if two strings are equal. 
 * Any of strings is NULL, return false. */
bool streq(char *str1, char *str2) {
    return str1 && str2 && strcmp(str1, str2) == 0;
}

/* Check if two strings are equal, 
 * if both are null, also return true. */
bool streq_or_null(char *str1, char *str2) {
    if (str1 && str2)
        return strcmp(str1, str2) == 0;
    else if (!str1 && !str2)
        return true;
    else
        return false;
}


/* Convert int32 to string. */
char *itos(int32_t val) {
    char *str = dalloc(MAX_INT_STR_LENGTH);
    sprintf(str, "%d", val);
    return str;
}

/* Convert long to string. */
char *ltos(int64_t val) {
    char *str = dalloc(MAX_LONG_STR_LENGTH);
    sprintf(str, "%ld", val);
    return str;
}

/* Covnert bool to string. */
char *btos(bool val) {
    char *str = dalloc(MAX_BOOL_STR_LENGTH);
    sprintf(str, val ? "true" : "false");
    return str;
}

/* Convert float to string. */
char *ftos(float val) {
    char *str = dalloc(MAX_FLOAT_STR_LENGTH);
    sprintf(str, "%f", val);
    return str;
}

/* Convert float to string. */
char *dtos(double val) {
    char *str = dalloc(MAX_DOUBLE_STR_LENGTH);
    sprintf(str, "%.15lf", val);
    return str;
}

/* Convert time to string. */
char *ttos(time_t val, char *frmt) {
    char temp[90];
    char *str = dalloc(MAX_TIMESTAMP_STR_LENGTH);
    struct tm *tmp_time = localtime(&val);
    strftime(temp, sizeof(temp), frmt, tmp_time);
    sprintf(str, "%s", temp);
    return str;
}

/* Convert String value to int32_t value.*/
ST_FLAG stoi32(char *val, int32_t *ret) {
    char buf[BUFF_SIZE];
    char *endptr;

    long converted = strtol(val, &endptr, 10);
    if (*endptr != '\0')
        return ST_INVALID;

    /* Check if overflow max int value. */
    if (converted > INT_MAX || converted < INT_MIN)
        return ST_OVERFLOW;

    /* Check if overflow max long value*/
    memset(buf, 0, BUFF_SIZE);
    sprintf(buf, "%ld", converted);
    if (!streq(val, buf))
        return ST_OVERFLOW;

    *ret =  (int32_t) converted;

    return ST_SUCCESS;
}


/* Convert String value to int64 value.
 * return 1 if success.
 * return 0 if not valid number.
 * return -1 if overflow.
 * */
ST_FLAG stoi64(char *val, int64_t *ret) {
    char buf[BUFF_SIZE];
    char *endptr;

    int64_t converted = strtol(val, &endptr, 10);
    if (*endptr != '\0')
        return ST_INVALID;

    /* Check if overflow max long value*/
    memset(buf, 0, BUFF_SIZE);
    sprintf(buf, "%ld", converted);
    if (!streq(val, buf))
        return ST_OVERFLOW;

    *ret = converted;

    return ST_SUCCESS;
}


/* Convert String value to float value.*/
ST_FLAG stof(char *val, float *ret) {
    char *endptr;
    errno = 0;
    double converted = strtod(val, &endptr);

    if (*endptr != '\0')
        return ST_INVALID;
    else if (errno == ERANGE)
        return ST_OUTRANGE;
    else if (isinff(converted))
        return ST_OVERFLOW;

    if (converted > FLT_MAX || converted < FLT_MIN) 
        return ST_OVERFLOW;

    *ret = (float) converted;

    return ST_SUCCESS;
}

/* Convert String value to double value. */
ST_FLAG stod(char *val, double *ret) {
    char *endptr;
    errno = 0;
    double converted = strtod(val, &endptr);

    if (*endptr != '\0')
        return ST_INVALID;
    else if (errno == ERANGE)
        return ST_OUTRANGE;
    else if (isinf(converted))
        return ST_OVERFLOW;

    if (fabs(converted) > DBL_MAX || fabs(converted) < DBL_MIN) 
        return ST_OVERFLOW;

    *ret = converted;

    return ST_SUCCESS;
}

 void show_bytes(byte_pointer start, size_t len) {
    size_t i;
    for (i = 0; i <len; i++)
        printf(" %.2x", start[i]);
    printf("\n");
}

/**********************************************************************/
/* Get a line from a socket, whether the line ends in a newline,
 * carriage return, or a CRLF combination.  Terminates the string read
 * with a null character.  If no newline indicator is found before the
 * end of the buffer, the string is terminated with a null.  If any of
 * the above three line terminators is read, the last character of the
 * string will be a linefeed and the string will be terminated with a
 * null character.
 * Parameters: the socket descriptor
 *             the buffer to save the data in
 *             the size of the buffer
 * Returns: the number of bytes stored (excluding null) */
/**********************************************************************/
int get_line(int sock, char *buf, int size) {
    int i = 0;
    char c = '\0';
    int n;

    while ((i < size - 1) && (c != '\n'))
    {
        n = recv(sock, &c, 1, 0);
        /* DEBUG printf("%02X\n", c); */
        if (n > 0)
        {
            if (c == '\r')
            {
                n = recv(sock, &c, 1, MSG_PEEK);
                /* DEBUG printf("%02X\n", c); */
                if ((n > 0) && (c == '\n'))
                    recv(sock, &c, 1, 0);
                else
                    c = '\n';
            }
            buf[i] = c;
            i++;
        }
        else
            c = '\n';
    }
    buf[i] = '\0';

    return(i);
}

/* Generate hascode. */
uint32_t hash_code(void *ptr) {
    int a = (uintptr_t)ptr & 0xFFFFFFF;
    /*return a ^ (a >> 16);*/
    a = (a ^ 61) ^ (a >> 16);
    a = a + (a << 3);
    a = a ^ (a >> 4);
    a = a * 0x27d4eb2d;
    a = a ^ (a >> 15);
    return a;
}

/* Table size .*/
uint32_t table_size_for(uint32_t cap) {
        uint32_t n = cap - 1;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return (n < 0) ? 1 : n;
}

/* Print binary. */
void print_binary(void *data, Size size) {
    for (int i = 0; i < size; i++) {
        printf("%02X\t", *((unsigned char *) data + i));
    }
    printf("\n");
}
