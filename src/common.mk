
ifdef DEBUG
	CFLAGS := -g -Wall $(foreach headerdir, $(headerdirs), -I$(headerdir)) -gdwarf-2 -g3 -D DEBUG
else
	CFLAGS := -O3 $(foreach headerdir, $(headerdirs), -I$(headerdir))
endif
