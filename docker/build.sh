#!/bin/bash

source ./docker/names.config

echo $WORKER_IMAGE_NAME

docker build -t $WORKER_IMAGE_NAME -f $WORKER_IMAGE_PATH .