CC = gcc
OBJS := $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))

include ../common.mk

LDADDCFLAGS = -lpthread -lm -lz

bench: bench.c 
	$(CC) $(CFLAGS) $< -o $@ $(LDADDCFLAGS)

clean:
	rm -f *.o bench
