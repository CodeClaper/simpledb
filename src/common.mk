
ifdef DEBUG
	CFLAGS := -g -Wall -O1 $(foreach headerdir, $(headerdirs), -I$(headerdir)) -gdwarf-2 -D DEBUG
else
	CFLAGS := -O3 $(foreach headerdir, $(headerdirs), -I$(headerdir))
endif
