#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/.apptainers/spark
PROCESSED_DATA=/scratch/fhgr-bigdata-bitcoin/mounted-data/processed-data
REDUCED_DATA=/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer shell  \
	--contain \
	--fakeroot \
	--dns=10.0.96.48,10.0.96.49 \
	--net --network=fakeroot \
	--writable \
	--mount type=bind,source=$PROCESSED_DATA,dst=/processed-data \
	--mount type=bind,source=$REDUCED_DATA,dst=/reduced-data \
	container/sandbox \
	spark
