
includedir = ../../include
headerdirs = $(includedir)/backend $(includedir)/common $(includedir)/storage \
			 $(includedir)/memory $(includedir)/utils $(includedir)/trans \
			 $(includedir)/sys $(includedir)/parser $(includedir)/lock
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
