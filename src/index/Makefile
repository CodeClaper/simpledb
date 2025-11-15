includedir = ../../include
headerdirs = $(includedir)/index $(includedir)/memory $(includedir)/common $(includedir)/sys $(includedir)/backend \
			 $(includedir)/utils $(includedir)/storage $(includedir)/ltree $(includedir)/lock $(includedir)/trans $(includedir)/heap
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
