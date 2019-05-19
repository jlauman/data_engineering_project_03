#!/usr/bin/env bash

chmod 600 ./bin/postgres.txt

export PGUSER=postgres
export PGPASSWORD=$(cat ./bin/postgres.txt)

rm -rf ./data/postgres
mkdir -p ./data/postgres

docker run \
    --name postgres \
    --net datanet \
    --publish 5432:5432 \
    --restart unless-stopped \
    --mount type=bind,source="$(pwd)/data/postgres",target=/var/lib/postgresql/data \
    --env POSTGRES_PASSWORD=$PGPASSWORD \
    --detach \
    postgres:11.2-alpine

# should use pg_ctl to check status of postgres server
sleep 20

docker cp ./bin/pg_setup.sh postgres:/opt/

docker exec \
    --env PGUSER=$PGUSER \
    --env PGPASSWORD=$PGPASSWORD \
    --workdir /opt \
    postgres \
    /usr/bin/env sh ./pg_setup.sh
