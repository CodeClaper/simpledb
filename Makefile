SUBDIRS := src test/gtest test/minunit

single:
	$(MAKE) -C src

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
	@$(MAKE) -C test/minunit check

clean: 
	@$(foreach dir, $(SUBDIRS), $(MAKE) -C $(dir) clean;)
