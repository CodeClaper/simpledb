
includedir = ../../include
headerdirs = $(includedir)/trans $(includedir)/common $(includedir)/memory $(includedir)/sys \
			 $(includedir)/utils $(includedir)/storage $(includedir)/backend $(includedir)/lock

headers := $(shell find $(headerdirs) -name "*.h")
objects := $(patsubst %.c,%.o,$(wildcard *.c))
CC = gcc
CFLAGS = $(foreach headerdir, $(headerdirs), -I$(headerdir))

include ../common.mk

all: $(objects)

%.o: %.c $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY: clean all

clean:
	rm -f *.o
