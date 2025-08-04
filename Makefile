SUBDIRS := src test/gtest

all: 
	$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir);)

.PHONY: all stat check check-pytest check_gtest clean

stat:
	cloc src include 

check: check-gtest check-pytest

check-pytest:
	pytest -v 

check-gtest:
	@$(MAKE) -C test/gtest check

clean: 
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)
