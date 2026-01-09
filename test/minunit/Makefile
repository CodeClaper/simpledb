includedir = ../../include
headerdirs = $(includedir)/optimizer $(includedir)/common $(includedir)/storage $(includedir)/backend \
			 $(includedir)/memory $(includedir)/utils $(includedir)/trans $(includedir)/ltree $(includedir)/index \
		     $(includedir)/rid $(includedir)/sid $(includedir)/sys $(includedir)/parser $(includedir)/lock $(includedir)/heap
headers := $(shell find $(headerdirs) -name "*.h")
objects := $(patsubst %.c,%.o,$(wildcard *.c))
CC = gcc

test: $(objects)
	$(CC) -g3 -lrt -lm $(objects) -D DEBUG -o test

%.o: %.c $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY: clean all

clean:
	rm -f *.o test
