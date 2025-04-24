#ifndef BUFPOOL_H
#define BUFPOOL_H

#include "bufmgr.h"
#include <stdbool.h>

/* Create the Buffer Pool. */
void CreateBufferPool();

/* Get Block*/
void *GetBufferBlock(Buffer buffer);


/* Read Buffer Block. */
void BufferReadBlock(BufferTag *tag, Buffer buffer);


/* Write Buffer Block. 
 * If realy write to file, return true.
 * */
bool BufferWriteBlock(Buffer buffer);

#endif
