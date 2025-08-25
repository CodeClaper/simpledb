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

/* Left trim*/
char *LeftTrim(char *s);

/* Right trim */
char *RightTrim(char *s);

/* trim */
char *Trim(char *s);

/* Check if a string contains substring.*/
bool Contains(char* str, char *substr);

/* Check if a file has prefix. */
bool StartWith(char *str, const char *prefix);

/* Check if a file has suffix. */
bool EndWith(char *str, char *suffix);

/* Substring */
char *SubStr(char *str, uint32_t start, uint32_t end);

/* Replace once */
char *ReplaceOnce(char *str, const char *old_str, const char *new_str);

/* Replace all. */
char *ReplaceAll(char *str, char *old_str, char *new_str);

/* Check if empty string. */
bool StrIsEmpty(char *str);

/* Check if str is date format. */
bool StrIsDate(char *str);

/* Check if str is timestamp format. */
bool StrIsTimestamp(char *str);

/* Format String and return. */
char *FormatStr(char *format, ...);

/* Check both string if equal. */
bool StrEq(char *str1, char *str2);

/* Check if two strings are equal, ignoring case. */
bool StrNoCaseEq(char *str1, char *str2);

/* Check both string if equal, 
 * if both is null, also return true. */
bool StrEqOrNull(char *str1, char *str2);

/* String concat. */
char *StrCat(char *str1, char *str2);

/* Convert int32 to string. */
char *IntToStr(int32_t val);

/* Convert long to string. */
char *LongToStr(int64_t val);

/* Covnert bool to string. */
char *BoolToStr(bool val);

/* Convert float to string. */
char *FloatToStr(float val);

/* Convert float to string. */
char *DoubleToStr(double val);

/* Convert time to string. */
char *TimeToStr(time_t val, char *frmt);

/* Convert String value to int32_t value. */
ST_FLAG StrToInt(char *val, int32_t *ret);

/* Convert String value to int64 value.*/
ST_FLAG StrToLong(char *val, int64_t *ret);

/* Convert String value to float value. */
ST_FLAG StrToFloat(char *val, float *ret);

/* Convert String value to double value. */
ST_FLAG StrToDouble(char *val, double *ret);

/* Convert String value to bool value. */
ST_FLAG StrToBool(char *val, bool *ret);

/* Escap the string value. */
char *EscapStr(const char *str);

/* Get line from socket. */
int get_line(int sock, char *buf, int size);

/* Return true if pointer is NULL. */
static inline bool IsNull(void *ptr) {
    return ptr == NULL;
}

/* Return true if pointer is not NULL. */
static inline bool NonNull(void *ptr) {
    return ptr != NULL;
}


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
