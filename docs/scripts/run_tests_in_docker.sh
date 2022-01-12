#!/bin/bash

cd `dirname "${BASH_SOURCE[0]}"`
docker-compose -f ../../../../docker-compose-dev.yml  run --rm   complex_rest python /complex_rest/complex_rest/manage.py test /complex_rest/plugin_dev/otl_interpreter/tests  --settings=core.settings.test -k test_unregister
docker-compose -f ../../../../docker-compose-dev.yml down