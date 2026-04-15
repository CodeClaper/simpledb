topdir = ../../
includedir = $(topdir)include
srcdir = $(topdir)src
headerdirs = $(includedir)/db $(includedir)/optimizer $(includedir)/common $(includedir)/storage $(includedir)/backend \
			 $(includedir)/memory $(includedir)/utils $(includedir)/trans $(includedir)/ltree $(includedir)/index \
		     $(includedir)/rid $(includedir)/sid $(includedir)/sys $(includedir)/parser $(includedir)/lock $(includedir)/heap
SUBDIRS := $(srcdir)/db $(srcdir)/backend  $(srcdir)/common $(srcdir)/lock $(srcdir)/heap \
		   $(srcdir)/memory $(srcdir)/parser $(srcdir)/storage $(srcdir)/sys\
		   $(srcdir)/trans $(srcdir)/utils $(srcdir)/ltree $(srcdir)/sid $(srcdir)/rid $(srcdir)/index $(srcdir)/optimizer
headers := $(shell find $(headerdirs) -name "*.h")
objects := $(patsubst %.c,%.o,$(wildcard *.c)) $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))
CC = gcc

include ../../src/common.mk

LDADDCFLAGS = -lm -lrt -lz

test: $(objects)
	$(CC) $(CFLAGS) $(objects) -o test $(LDADDCFLAGS)

%.o: %.c $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY: clean all

clean:
	rm -f *.o test
