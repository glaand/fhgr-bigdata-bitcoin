#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/apptainers/spark-cluster
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer instance start --contain --writable --mount type=bind,source=/scratch/fhgr-bigdata-bitcoin/output,dst=/output --mount type=bind,source=$(pwd)/data,dst=/data --mount type=bind,source=/scratch/fhgr-bigdata-bitcoin/raw-data,dst=/raw-data container/sandbox spark-cluster
