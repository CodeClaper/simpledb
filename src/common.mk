
ifdef DEBUG
	CFLAGS := -g -Wall -O3 $(foreach headerdir, $(headerdirs), -I$(headerdir)) -g3 -gdwarf-2 -D DEBUG
else
	CFLAGS := -O3 $(foreach headerdir, $(headerdirs), -I$(headerdir))
endif
