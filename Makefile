
#.SILENT:
SHELL = /bin/bash

.PHONY: clean clean_build clean_pack clean_test clean_docker_test clean_venv test docker_test clean_otl_interpreter_venv install_conda install_conda_pack

all:
	echo -e "Required section:\n\
 build - build project into build directory, with configuration file and environment\n\
 clean - clean all addition file, build directory and output archive file\n\
 test - run all tests\n\
 pack - make output archive, file name format \"otl_interpreter_vX.Y.Z_BRANCHNAME.tar.gz\"\n\
Addition section:\n\
 venv\n\
 install_conda - install miniconda in local directory \n\
 install_conda_pack - install conda pack in local directory\n\
"


VERSION := $(shell cat setup.py | grep __version__ | head -n 1 | sed -re "s/[^\"']+//" | sed -re "s/[\"',]//g")
BRANCH := $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')

CONDA = conda/miniconda/bin/conda
ENV_PYTHON = venv/bin/python3.9

define clean_docker_containers
	@echo "Stopping and removing docker containers"
	docker network prune -f
	docker-compose -f docker-compose-test.yml stop
	-if [[ $$(docker ps -aq -f name=otl_interpreter) ]]; then docker rm -f $$(docker ps -aq -f name=otl_interpreter);  fi;
endef

conda/miniconda.sh:
	echo Download Miniconda
	mkdir -p conda
	wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh -O conda/miniconda.sh; \

conda/miniconda: conda/miniconda.sh
	bash conda/miniconda.sh -b -p conda/miniconda; \

install_conda: conda/miniconda

conda/miniconda/bin/conda-pack: conda/miniconda
	conda/miniconda/bin/conda install conda-pack -c conda-forge  -y

install_conda_pack: conda/miniconda/bin/conda-pack

clean_conda:
	rm -rf ./conda

pack: make_build
	$(SET_VERSION)
	$(SET_BRANCH)
	rm -f otl_interpreter-*.tar.gz
	echo Create archive \"otl_interpreter-$(VERSION)-$(BRANCH).tar.gz\"
	cd make_build; tar czf ../otl_interpreter-$(VERSION)-$(BRANCH).tar.gz otl_interpreter ot_simple_rest_job_proxy

clean_pack:
	rm -f otl_interpreter-*.tar.gz


otl_interpreter.tar.gz: build
	cd make_build; tar czf ../otl_interpreter.tar.gz otl_interpreter && rm -rf ../make_build

build: make_build

make_build: venv venv.tar.gz
	# required section
	echo make_build
	mkdir -p make_build

	cp -R ./otl_interpreter make_build
	cp -R ./ot_simple_rest_job_proxy make_build
	rm -f make_build/otl_interpreter/otl_interpreter.conf
	cp *.md make_build/otl_interpreter/
	cp *.py make_build/otl_interpreter/
	cp ./docs/scripts/create_db.sh make_build/otl_interpreter/
	mkdir -p make_build/otl_interpreter/venv
	tar -xzf ./venv.tar.gz -C make_build/otl_interpreter/venv

clean_build:
	rm -rf make_build

venv: conda/miniconda
	echo Create venv
	$(CONDA) create --copy -p ./venv -y
	$(CONDA) install -p ./venv python==3.9.7 -y
	$(ENV_PYTHON) -m pip install --no-input  -r requirements.txt 	--extra-index-url http://s.dev.isgneuro.com/repository/ot.platform/simple --trusted-host s.dev.isgneuro.com

venv.tar.gz: venv conda/miniconda/bin/conda-pack
	$(CONDA) pack -p ./venv -o ./venv.tar.gz

otl_interpreter/venv: venv.tar.gz
	mkdir -p otl_interpreter/venv
	tar -xzf ./venv.tar.gz -C otl_interpreter/venv

clean_otl_interpreter_venv:
	rm -rf otl_interpreter/venv

clean_venv:
	rm -rf venv
	rm -f ./venv.tar.gz


clean: clean_build clean_venv clean_pack clean_test clean_conda

test: docker_test

logs:
	mkdir -p ./logs

docker_test: logs otl_interpreter/venv
	$(call clean_docker_containers)
	@echo "Testing..."
	CURRENT_UID=$$(id -u):$$(id -g) docker-compose -f docker-compose-test.yml run --rm  complex_rest python ./complex_rest/manage.py test ./tests --settings=core.settings.test --no-input
	$(call clean_docker_containers)

docker_dev: otl_interpreter/venv
	$(call clean_docker_containers)
	CURRENT_UID=$$(id -u):$$(id -g) docker-compose -f docker-compose-dev.yml up -d

docker_dev_stop:
	CURRENT_UID=$$(id -u):$$(id -g) docker-compose -f docker-compose-dev.yml stop

clean_docker_test:
	$(call clean_docker_containers)

clean_test: clean_docker_test clean_otl_interpreter_venv







