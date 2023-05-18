#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/.apptainers/spark
set -e
mkdir -p $(pwd)/container/sandbox
mkdir -p $(pwd)/container/cache
mkdir -p $(pwd)/container/tmp
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer build --force --fakeroot --nv --sandbox container/sandbox docker://apache/spark-py
mkdir $(pwd)/container/sandbox/processed-data
mkdir $(pwd)/containers/sandbox/reduced-data

