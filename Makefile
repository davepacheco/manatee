#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

JSL		 = ./deps/javascriptlint/build/install/jsl
JSL_CONF_NODE	 = ./tools/jsl.node.conf
JSSTYLE		 = ./deps/jsstyle/jsstyle
JSSTYLE_FLAGS	 = -f ./tools/jsstyle.conf
NPM		 = npm
NODE	 	 = node

BASH_FILES	 = tools/mkdevsitters
JS_FILES	:= \
	$(wildcard ./*.js ./lib/*.js ./test/*.js) \
	bin/manatee-adm \
	tools/mksitterconfig
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
SMF_MANIFESTS    = \
	smf/backupserver.xml \
	smf/sitter.xml \
	smf/snapshotter.xml

JSON_FILES	 = \
    $(wildcard ./etc/*.json ./test/etc/*.json) \
    package.json

include Makefile.defs
include Makefile.smf.defs

all:
	$(NPM) install

#
# No doubt other tests under test/ should be included by this, but they're not
# well enough documented at this point to incorporate.
#
test: all
	$(NODE) test/manateeAdmUsage.test.js
	@echo tests okay

include Makefile.deps
include Makefile.targ
include Makefile.smf.targ
