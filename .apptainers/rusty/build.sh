#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/.apptainers/rusty
set -e
mkdir -p $(pwd)/container/sandbox
mkdir -p $(pwd)/container/cache
mkdir -p $(pwd)/container/tmp
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer build --force --fakeroot --nv --sandbox container/sandbox docker://rust:latest
mkdir $(pwd)/container/sandbox/raw-data
mkdir $(pwd)/containers/sandbox/processed-data
