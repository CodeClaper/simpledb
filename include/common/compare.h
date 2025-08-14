#include "data.h"

/* The EQ operation. */
bool EQ(void *source, void *target, DataType data_type);

/* The NE operation. */
bool NE(void *source, void *target, DataType data_type);

/* The GT operation. */
bool GT(void *source, void *target, DataType data_type);

/* The GE operation. */
bool GE(void *source, void *target, DataType data_type);

/* The LT operation. */
bool LT(void *source, void *target, DataType data_type);

/* The LE operation. */
bool LE(void *source, void *target, DataType data_type);

/* Eval */
bool eval(CompareType compare_type, void *source, void *target, DataType data_type);

/* Compare key values. */
bool KeyValueEval(CompareType compare_type, KeyValue *left, KeyValue *right);

/* Compare. */
int compare(void *source, void *taget, DataType data_type);
