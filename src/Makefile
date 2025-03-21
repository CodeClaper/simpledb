includedir = ../include
SUBDIRS := backend common lock memory parser storage trans utils
CC = gcc
OBJS := $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))

headerdirs = $(includedir)/common $(includedir)/lock $(includedir)/memory $(includedir)/parser \
			 $(includedir)/backend $(includedir)/storage $(includedir)/trans $(includedir)/utils

headers := $(shell find $(headerdirs) -name "*.h")

include ./common.mk

LDADDCFLAGS = -lpthread -lm -lz

all: $(SUBDIRS) simpledb simpledb_cli

simpledb: db.o $(OBJS)
	$(CC) $(CFLAGS)  db.o $(OBJS) -o $@ $(LDADDCFLAGS)

simpledb_cli: db_cli.o
	$(CC) $(CFLAGS) db_cli.o -o $@ -lreadline -ltinfo

db.o: db.c $(headers)
	$(CC) $(CFLAGS) -c db.c -o $@

db_cli.o: db_cli.c
	$(CC) $(CFLAGS) -c db_cli.c -o $@

$(SUBDIRS): 
	@$(MAKE) -C $@

.PHONY: clean $(SUBDIRS) all

clean:
	rm -f *.o  simpledb simpledb_cli
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)

