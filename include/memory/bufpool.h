#include "bufmgr.h"
#include <stdbool.h>

#ifndef __BUFPOOL_H__
#define __BUFPOOL_H__


void CreateBufferPool();
void *GetBufferBlock(Buffer buffer);
void BufferReadBlock(BufferTag *tag, Buffer buffer);
bool BufferWriteBlock(Buffer buffer);

#endif
