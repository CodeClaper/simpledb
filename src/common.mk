
ifdef DEBUG
	CFLAGS := -g -Wall -O0 $(foreach headerdir, $(headerdirs), -I$(headerdir)) -D DEBUG
else
	CFLAGS := -O3 $(foreach headerdir, $(headerdirs), -I$(headerdir))
endif
