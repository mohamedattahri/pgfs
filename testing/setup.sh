#!/usr/bin/env sh

set -e

DOCKER_IMAGE=postgres:14-alpine

echo Creating database container...
CONTAINER_ID=`docker run --detach --env POSTGRES_DB=$POSTGRES_DB --env POSTGRES_USER=$POSTGRES_USER --env POSTGRES_PASSWORD=$POSTGRES_PASSWORD --publish 5432:$POSTGRES_PORT $DOCKER_IMAGE`

cleanup() {
  echo Removing container $CONTAINER_ID...
  eval "docker rm --force $CONTAINER_ID &> /dev/null"
  exit 0
}

trap cleanup EXIT

# Run wrapped command
eval $@
