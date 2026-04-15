includedir = ../../include
headerdirs = $(includedir)/main $(includedir)/common $(includedir)/json $(includedir)/lock $(includedir)/memory $(includedir)/ltree $(includedir)/parser \
			 $(includedir)/sys $(includedir)/backend $(includedir)/storage $(includedir)/trans $(includedir)/utils $(includedir)/optimizer
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
