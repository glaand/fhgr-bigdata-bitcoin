#!/bin/bash
PARSER_DIR=/data/BlockReader/reader.bin
apptainer exec instance://spark-cluster $PARSER_DIR /raw-data/.bitcoin/blocks $1 $2
