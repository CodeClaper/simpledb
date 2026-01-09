SUBDIRS := src test/gtest test/minunit

all: 
	$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir);)

.PHONY: all stat check check-pytest check_gtest clean

stat:
	cloc src include 

check: check-gtest check-pytest check-minunit

check-pytest:
	pytest -v 

check-gtest:
	@$(MAKE) -C test/gtest check

check-minunit:
	cd test/minunit && ./test

clean: 
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)
