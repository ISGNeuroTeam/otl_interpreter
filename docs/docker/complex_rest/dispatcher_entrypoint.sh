#!/bin/sh

echo "Waiting for postgres and kafka..."
sleep 90


exec "$@"