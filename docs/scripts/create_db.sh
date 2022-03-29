#!/bin/bash

export PGUSER=postgres
export PGPASSWORD=postgres
export PGPORT=5433

psql -U postgres -h localhost -p 5433 << EOF
create user otl_interpreter with password 'otl_interpreter';
create database otl_interpreter;
grant all privileges on database otl_interpreter to otl_interpreter;
EOF

./venv/bin/python ./complex_rest/manage.py migrate otl_interpreter --database=otl_interpreter