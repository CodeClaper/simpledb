
includedir = ../../include
headerdirs = $(includedir)/lock $(includedir)/common $(includedir)/memory $(includedir)/sys \
			 $(includedir)/storage $(includedir)/utils
objects := $(patsubst %.c,%.o,$(wildcard *.c))
headers := $(shell find $(headerdirs) -name "*.h")
CC = gcc

include ../common.mk

all: $(objects)

%.o: %.c $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY: clean all

clean:
	rm -f *.o
