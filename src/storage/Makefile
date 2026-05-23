
includedir = ../../include
headerdirs = $(includedir)/storage $(includedir)/common $(includedir)/memory $(includedir)/sys $(includedir)/ltree \
			 $(includedir)/trans $(includedir)/backend $(includedir)/utils $(includedir)/lock $(includedir)/optimizer \
			 $(includedir)/heap $(includedir)/rid $(includedir)/sid $(includedir)/index 
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
