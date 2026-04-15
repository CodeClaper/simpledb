topdir = ../../
includedir = $(topdir)include
srcdir = $(topdir)src
SUBDIRS := $(srcdir)/db $(srcdir)/backend $(srcdir)/common $(srcdir)/lock $(srcdir)/heap \
		   $(srcdir)/memory $(srcdir)/parser $(srcdir)/storage $(srcdir)/sys\
		   $(srcdir)/trans $(srcdir)/utils $(srcdir)/sid $(srcdir)/rid $(srcdir)/ltree $(srcdir)/optimizer $(srcdir)/index
objects := $(patsubst %.cpp, %.o, $(wildcard *.cpp)) $(patsubst %.c, %.o, $(shell find $(SUBDIRS) -name "*.c"))

CPPLDADD = -lgtest_main -lgtest
CPPLDFLAGS= -pthread -lz
ifdef DEBUG
	CPPFLAGS := -g -O0 -Wall -I/usr/src/googletest/googletest/include -I/usr/src/googletest/googletest \
		   -I$(includedir)/db -I$(includedir)/lock -I$(includedir)/common -I$(includedir)/utils -I$(includedir)/memory \
		   -I$(includedir)/heap -I$(includedir)/backend -I$(includedir)/trans -I$(includedir)/storage -I$(includedir)/storage \
		   -I$(includedir)/sys -I$(includedir)/sid -I$(includedir)/rid -I$(includedir)/ltree -I$(includedir)/optimizer -I$(includedir)/index \
		   -I$(includedir)/parser -I/usr/local/include/gtest -D DEBUG
else
	CPPFLAGS := -g -O3 -Wall -I/usr/src/googletest/googletest/include -I/usr/src/googletest/googletest \
		   -I$(includedir)/db -I$(includedir)/lock -I$(includedir)/common -I$(includedir)/utils -I$(includedir)/memory \
		   -I$(includedir)/heap -I$(includedir)/backend -I$(includedir)/trans -I$(includedir)/storage -I$(includedir)/storage \
		   -I$(includedir)/sys -I$(includedir)/sid -I$(includedir)/rid -I$(includedir)/ltree -I$(includedir)/optimizer -I$(includedir)/index \
		   -I$(includedir)/parser -I/usr/local/include/gtest
endif


target = gtest

all: $(target)

$(target): $(objects)
	$(CXX) $(CPPFLAGS) $(objects) -o $(target) $(CPPLDADD) $(CPPLDFLAGS)

%o: %.cpp
	$(CXX) $(CPPFLAGS) -c $< -o $@ $(CPPLDADD) $(CPPLDFLAGS)

.PHONY: all clean check

clean:
	rm -f *.o $(target)

check:$(target)
	@./gtest

