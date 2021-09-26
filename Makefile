
#.SILENT:
SHELL = /bin/bash


all:
	echo -e "Required section:\n\
 build - build project into build directory, with configuration file and environment\n\
 clean - clean all addition file, build directory and output archive file\n\
 test - run all tests\n\
 pack - make output archive, file name format \"otl_interpreter_vX.Y.Z_BRANCHNAME.tar.gz\"\n\
Addition section:\n\
 venv\n\
"

GENERATE_VERSION = $(shell cat setup.py | grep __version__ | head -n 1 | sed -re 's/[^"]+//' | sed -re 's/"//g' )
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')
SET_VERSION = $(eval VERSION=$(GENERATE_VERSION))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))

pack: make_build
	$(SET_VERSION)
	$(SET_BRANCH)
	rm -f otl_interpreter-*.tar.gz
	echo Create archive \"otl_interpreter-$(VERSION)-$(BRANCH).tar.gz\"
	cd make_build; tar czf ../otl_interpreter-$(VERSION)-$(BRANCH).tar.gz otl_interpreter

clean_pack:
	rm -f otl_interpreter-*.tar.gz


otl_interpreter.tar.gz: build
	cd make_build; tar czf ../otl_interpreter.tar.gz otl_interpreter && rm -rf ../make_build

build: make_build

make_build: venv venv_pack
	# required section
	echo make_build
	mkdir make_build

	cp -R ./otl_interpreter make_build
	rm make_build/otl_interpreter/otl_interpreter.conf
	mv make_build/otl_interpreter/otl_interpreter.conf.example make_build/otl_interpreter/otl_interpreter.conf
	cp *.md make_build/otl_interpreter/
	cp *.py make_build/otl_interpreter/
	mkdir make_build/otl_interpreter/venv
	tar -xzf ./venv.tar.gz -C make_build/otl_interpreter/venv

clean_build:
	rm -rf make_build

venv: clean_venv
	echo Create venv
	conda create --copy -p ./venv -y
	conda install -p ./venv python==3.8.5 -y
	./venv/bin/pip install --no-input  -r requirements.txt

venv_pack: venv
	conda pack -p ./venv -o ./venv.tar.gz

clean_venv:
	rm -rf venv
	rm -f ./venv.tar.gz


complex_rest:
	git clone git@github.com:ISGNeuroTeam/complex_rest.git
	{ cd ./complex_rest; git checkout develop; make venv; make redis; }
	ln -s ../../../../otl_interpreter/otl_interpreter ./complex_rest/complex_rest/plugins/otl_interpreter

clean_complex_rest:
ifneq (,$(wildcard ./complex_rest))
	{ cd ./complex_rest; make clean;}
	rm -f ./complex_rest/plugins/otl_interpreter
	rm -rf ./complex_rest
endif

clean: clean_build clean_venv clean_pack clean_test clean_complex_rest

test: venv complex_rest
	@echo "Testing..."
	./complex_rest/venv/bin/python ./complex_rest/complex_rest/manage.py test ./tests --settings=core.settings.test

clean_test: clean_complex_rest
	@echo "Clean tests"






