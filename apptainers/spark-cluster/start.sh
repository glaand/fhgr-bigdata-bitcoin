#!/bin/bash

export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer instance start --writable-tmpfs --mount type=bind,source=$(pwd)/data,dst=/data container/sandbox spark-cluster