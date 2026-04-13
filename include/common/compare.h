#include "data.h"

bool EQ(void *source, void *target, DataType data_type);
bool NE(void *source, void *target, DataType data_type);
bool GT(void *source, void *target, DataType data_type);
bool GE(void *source, void *target, DataType data_type);
bool LT(void *source, void *target, DataType data_type);
bool LE(void *source, void *target, DataType data_type);
bool eval(CompareType compare_type, void *source, void *target, DataType data_type);
bool KeyValueEval(CompareType compare_type, KeyValue *left, KeyValue *right);
int compare(void *source, void *taget, DataType data_type);
