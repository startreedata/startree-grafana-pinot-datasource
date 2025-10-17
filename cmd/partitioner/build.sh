#!/bin/bash

set -xe

IMAGE="repo.startreedata.io/startree-docker-registry/jackson-metrics-partitioner:latest"

GOOS=linux GOARCH=amd64 go build -o partitioner .

docker buildx build \
  --load \
  --platform linux/amd64 \
  --tag "${IMAGE}" \
  .

docker push "${IMAGE}"
