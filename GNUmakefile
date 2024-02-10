# Copyright (c) 2011, Couchbase, Inc.
# All rights reserved.

# Note: This Makefile is provided as a convenience wrapper to CMake,
# which is the build tool used for configuring this project. Please do
# not make any substantive changes only in this file or in the
# top-level "configure" script, as the normal process of building
# Couchbase server uses only CMake.

SHELL=/bin/sh

ifeq (,$(wildcard build))
    $(error "you need to run ./configure with --prefix option to be able to run ns_server")
endif

include build/config.mk

all:
	cd build && $(MAKE) --no-print-directory all

TEST_TARGETS = test test_eunit test_triq

.PHONY: $(TEST_TARGETS) ui_test

clean clean_all:
	cd build && $(MAKE) --no-print-directory clean ns_realclean

install:
	cd build && $(MAKE) --no-print-directory $@

$(TEST_TARGETS): edts_hack_clean

dataclean distclean $(TEST_TARGETS) ui_test dialyzer:
	cd build && $(MAKE) --no-print-directory ns_$@

minify:
	cd build/deps/gocode && $(MAKE) --no-print-directory ns_minify/fast

COUCHDB_FLAT_PROJECTS = couchdb ejson erlang-oauth etap \
                        lhttpc mapreduce mochiweb snappy
$(addprefix deps/,$(COUCHDB_FLAT_PROJECTS)):
	mkdir $@
	cd $@ && \
	  ln -s $(COUCHDB_SRC_DIR)/src/$(notdir $@) src && \
	  ln -s $(COUCHDB_BIN_DIR)/src/$(notdir $@) ebin

COUCHDB_NONFLAT_PROJECTS = couch_dcp couch_index_merger couch_set_view
$(addprefix deps/,$(COUCHDB_NONFLAT_PROJECTS)):
	mkdir $@
	cd $@ && \
	  ln -s $(COUCHDB_SRC_DIR)/src/$(notdir $@)/src src && \
	  ln -s $(COUCHDB_SRC_DIR)/src/$(notdir $@)/include include && \
	  ln -s $(COUCHDB_BIN_DIR)/src/$(notdir $@)/ebin ebin

COUCHDB_PROJECTS = $(COUCHDB_FLAT_PROJECTS) $(COUCHDB_NONFLAT_PROJECTS)
COUCHDB_PROJECTS_DEPS_DIRS = $(addprefix deps/,$(COUCHDB_PROJECTS))

.PHONY: edts_hack edts_hack_clean
edts_hack: $(COUCHDB_PROJECTS_DEPS_DIRS)

edts_hack_clean:
	rm -rf $(COUCHDB_PROJECTS_DEPS_DIRS)

ifneq (,$(ENABLE_EDTS_HACK))
all: edts_hack
clean clean_all: edts_hack_clean
endif

# assuming exuberant-ctags
TAGS:
	ctags -eR .
