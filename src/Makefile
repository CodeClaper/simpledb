includedir = ../include
SUBDIRS := db backend common optimizer json lock memory ltree index rid sid parser heap storage trans utils sys
CC = gcc
OBJS := $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))

headerdirs = $(includedir)/db $(includedir)/common $(includedir)/json $(includedir)/lock $(includedir)/memory $(includedir)/ltree $(includedir)/parser \
			 $(includedir)/sys $(includedir)/backend $(includedir)/storage $(includedir)/trans $(includedir)/utils $(includedir)/optimizer

headers := $(shell find $(headerdirs) -name "*.h")

include ./common.mk

LDADDCFLAGS = -lpthread -lm -lz

all: $(SUBDIRS) simpledb  bench

simpledb: main.o $(OBJS)
	$(CC) $(CFLAGS) main.o $(OBJS) -o $@ $(LDADDCFLAGS)

bench:
	@$(MAKE) -C $@

main.o: main.c $(headers)
	$(CC) $(CFLAGS) -c main.c -o $@

$(SUBDIRS): 
	@$(MAKE) -C $@

.PHONY: clean $(SUBDIRS) all bench

clean:
	rm -f *.o  simpledb simpledb_cli
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)
	$(MAKE) -C bench clean

