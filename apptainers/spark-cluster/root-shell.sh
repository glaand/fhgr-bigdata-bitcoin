#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/apptainers/spark-cluster
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer shell --contain --fakeroot --dns=10.0.96.48,10.0.96.49 --net --network=fakeroot --writable --mount type=bind,source=$(pwd)/data,dst=/data container/sandbox spark-cluster
