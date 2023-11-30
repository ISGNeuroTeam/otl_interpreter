#!/bin/sh

echo "Waiting for postgres and kafka..."
sleep 90


python ./complex_rest/manage.py flush --no-input

python ./complex_rest/manage.py migrate --database=auth_db
python ./complex_rest/manage.py migrate
python ./complex_rest/manage.py createcachetable --database=auth_db
python ./complex_rest/manage.py createcachetable
echo "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('admin', '', 'admin')" | python /complex_rest/complex_rest/manage.py shell


python ./complex_rest/manage.py migrate otl_interpreter --database=otl_interpreter

exec "$@"