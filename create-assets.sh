#!/bin/sh

set -ex
source .env
brew install minio/stable/mc

mc alias set local http://127.0.0.1:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
mc admin info local

mc mb local/streets-data
#mc ls local/streets-data