CC = gcc
includedir = ../../include
SUBDIRS := ../json ../socket
OBJS := $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))

headerdirs = $(includedir)/json $(includedir)/socket

headers := $(shell find $(headerdirs) -name "*.h")

include ../common.mk

LDADDCFLAGS = -lpthread -lm -lz

all: $(SUBDIRS) bench

bench: $(OBJS) $(headers) bench.c 
	$(CC) $(CFLAGS) bench.c $(OBJS) $(headers) -o $@ $(LDADDCFLAGS)

$(SUBDIRS): 
	@$(MAKE) -C $@

.PHONY: clean $(SUBDIRS) all

clean:
	rm -f *.o bench
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)
