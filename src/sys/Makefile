
includedir = ../../include
headerdirs = $(includedir)/sys $(includedir)/common $(includedir)/memory $(includedir)/storage \
			 $(includedir)/utils $(includedir)/lock $(includedir)/backend $(includedir)/trans
headers := $(shell find $(headerdirs) -name "*.h")
objects := $(patsubst %.c,%.o,$(wildcard *.c))
CC = gcc

include ../common.mk

all: $(objects)

%.o: %.c $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY: clean all

clean:
	rm -f *.o
