
includedir = ../../include
headerdirs = $(includedir)/parser $(includedir)/common $(includedir)/memory $(includedir)/lock \
			 $(includedir)/sys $(includedir)/utils  $(includedir)/trans
headers := $(shell find $(headerdirs) -name "*.h")
objects := $(patsubst %.c,%.o,$(wildcard *.c))
CC = gcc

include ../common.mk

all: $(objects)

%.o: %.c $(headers) y.tab.c
	$(CC) -c $(CFLAGS) $< -o $@

y.tab.c: sql.l sql.y
	bison -d -y sql.y
	flex sql.l

PHONY: clean all

clean:
	rm -f *.o
