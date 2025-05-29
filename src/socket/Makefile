
includedir = ../../include
headerdirs = $(includedir)/socket $(includedir)/json
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
