#!/bin/bash

cd `dirname "${BASH_SOURCE[0]}"`
export CURRENT_UID=$(id -u):$(id -g)
docker-compose -f ../../../../docker-compose-test.yml  run --rm   complex_rest python /complex_rest/complex_rest/manage.py test /complex_rest/plugin_dev/otl_interpreter/tests   --settings=core.settings.test --no-input
docker-compose -f ../../../../docker-compose-test.yml down