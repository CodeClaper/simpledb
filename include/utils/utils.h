#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <stdio.h>
#include "c.h"

#ifndef UTILS_H
#define UTILS_H

typedef enum ST_FLAG {
    ST_SUCCESS = 0,
    ST_INVALID,
    ST_OVERFLOW,
    ST_OUTRANGE
} ST_FLAG;

typedef unsigned char *byte_pointer;

/* left trim*/
char *ltrim(char *s);

/* right trim */
char *rtrim(char *s);

/* trim */
char *trim(char *s);

/* Check if a string contains substring.*/
bool contains(char* str, char *substr);

/* Check if a file has prefix. */
bool startwith(char *str, const char *prefix);

/* Check if a file has suffix. */
bool endwith(char *str, char *suffix);

/* substring */
char *substr(char *str, uint32_t start, uint32_t end);

/* replace once */
char *replace_once(char *str, const char *old_str, const char *new_str);

/* Replace all. */
char *replace_all(char *str, char *old_str, char *new_str);

/* Check if empty string. */
bool is_empty(char *s);

/* Format String and return. */
char *format(char *format, ...);

/* Check both string if equal. */
bool streq(char *str1, char *str2);

/* Check both string if equal, 
 * if both is null, also return true. */
bool streq_or_null(char *str1, char *str2);

/* Return true if pointer is NULL. */
inline static bool is_null(void *ptr) {
    return ptr == NULL;
}

/* Return true if pointer is not NULL. */
inline static bool non_null(void *ptr) {
    return ptr != NULL;
}

/* Convert int32 to string. */
char *itos(int32_t val);

/* Convert long to string. */
char *ltos(int64_t val);

/* Covnert bool to string. */
char *btos(bool val);

/* Convert float to string. */
char *ftos(float val);

/* Convert float to string. */
char *dtos(double val);

/* Convert time to string. */
char *ttos(time_t val, char *frmt);


/* Convert String value to int32_t value. */
ST_FLAG stoi32(char *val,  int32_t *ret);


/* Convert String value to int64 value.*/
ST_FLAG stoi64(char *val,  int64_t *ret);


/* Convert String value to float value. */
ST_FLAG stof(char *val, float *ret);


/* Convert String value to double value. */
ST_FLAG stod(char *val, double *ret);

/* Get line from socket. */
int get_line(int sock, char *buf, int size);


/* Max size. */
inline static size_t max_size(size_t size1, size_t size2) {
    return size1 > size2 ? size1: size2;
}

/* Min size. */
inline static size_t min_size(size_t size1, size_t size2) {
    return size1 < size2 ? size1 : size2;
}


/* Show bytes. */
void show_bytes(byte_pointer start, size_t len);


/* Print binary. */
void print_binary(void *data, Size size);

#endif
