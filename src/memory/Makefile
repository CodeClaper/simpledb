includedir = ../../include
headerdirs = $(includedir)/memory $(includedir)/common $(includedir)/trans $(includedir)/sys $(includedir)/ltree \
			 $(includedir)/utils $(includedir)/lock $(includedir)/storage $(includedir)/heap $(includedir)/optimizer
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
