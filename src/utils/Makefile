
includedir = ../../include
headerdirs = $(includedir)/utils $(includedir)/common $(includedir)/memory \
			 $(includedir)/sys $(includedir)/lock $(includedir)/trans
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
