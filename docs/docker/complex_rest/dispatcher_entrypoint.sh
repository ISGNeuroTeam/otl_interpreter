#!/bin/sh

sleep 100

export PGPASSWORD=otl_interpreter
until psql  -c "select 1;" -U otl_interpreter -h postgres -c "select 1;" > /dev/null 2>&1; do
  echo "Waiting for postgres server"
  sleep 5
done
echo "Starting"

exec "$@"