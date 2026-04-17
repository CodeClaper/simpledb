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

char *LeftTrim(char *s);
char *RightTrim(char *s);
char *Trim(char *s);
bool Contains(char* str, char *substr);
bool StartWith(char *str, const char *prefix);
bool EndWith(char *str, char *suffix);
char *SubStr(char *str, uint32_t start, uint32_t end);
char *ReplaceOnce(char *str, const char *old_str, const char *new_str);
char *ReplaceAll(char *str, char *old_str, char *new_str);
bool StrIsEmpty(char *str);
bool StrIsDate(char *str);
bool StrIsTimestamp(char *str);
char *FormatStr(char *format, ...);
bool StrEq(char *str1, char *str2);
bool StrNoCaseEq(char *str1, char *str2);
bool StrEqOrNull(char *str1, char *str2);
char *StrCat(char *str1, char *str2);
char *IntToStr(int32_t val);
char *LongToStr(int64_t val);
char *BoolToStr(bool val);
char *FloatToStr(float val);
char *DoubleToStr(double val);
char *TimeToStr(time_t val, char *frmt);
ST_FLAG StrToInt(char *val, int32_t *ret);
ST_FLAG StrToLong(char *val, int64_t *ret);
ST_FLAG StrToFloat(char *val, float *ret);
ST_FLAG StrToDouble(char *val, double *ret);
ST_FLAG StrToBool(char *val, bool *ret);
char *EscapStr(const char *str);
int get_line(int sock, char *buf, int size);
void show_bytes(byte_pointer start, size_t len);
void print_binary(void *data, Size size);

/* Return true if pointer is NULL. */
static inline bool IsNull(void *ptr) { return ptr == NULL; }
/* Return true if pointer is not NULL. */
static inline bool NonNull(void *ptr) { return ptr != NULL; }
/* Max size. */
inline static size_t max_size(size_t size1, size_t size2) { return size1 > size2 ? size1: size2; }
/* Min size. */
inline static size_t min_size(size_t size1, size_t size2) { return size1 < size2 ? size1 : size2; }

#endif
