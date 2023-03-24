#!/bin/sh

# Wait for the Hadoop Datanode docker to be running
while ! nc -z hadoop-datanode 9864; do
  >&2 echo "Datanode is unavailable - sleeping"
  sleep 3
done
exit 0;
