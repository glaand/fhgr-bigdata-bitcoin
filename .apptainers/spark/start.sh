#!/bin/bash
cd /scratch/fhgr-bigdata-bitcoin/.apptainers/spark
PROCESSED_DATA=/scratch/fhgr-bigdata-bitcoin/mounted-data/processed-data
REDUCED_DATA=/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data
BTC_TO_USD=/scratch/fhgr-bigdata-bitcoin/btc_to_usd
PROCESSED_DATA_NEW=/scratch/fhgr-bigdata-bitcoin/mounted-data/processed-data-new
export APPTAINER_CACHEDIR=$(pwd)/container/cache
export APPTAINER_TMPDIR=$(pwd)/container/tmp
export TMPDIR=$(pwd)/container/tmp
apptainer instance start \
	--contain \
	--writable \
	--mount type=bind,source=$PROCESSED_DATA,dst=/processed-data \
        --mount type=bind,source=$PROCESSED_DATA_NEW,dst=/processed-data-new \
	--mount type=bind,source=$REDUCED_DATA,dst=/reduced-data \
	--mount type=bind,source=$BTC_TO_USD,dst=/btc_to_usd \
	--mount type=bind,source=/scratch,dst=/scratch \
	container/sandbox \
	spark
