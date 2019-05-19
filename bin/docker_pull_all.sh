#!/usr/bin/env bash

# docker pull alpine:3.9.2

docker pull postgres:11.2-alpine

docker network create -d bridge \
    --subnet 192.168.5.0/24 \
    --gateway 192.168.5.1 \
    datanet
