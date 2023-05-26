#!/bin/bash
apptainer exec instance://spark /opt/spark/bin/pyspark --executor-memory 8g --driver-memory 16g --num-executors 32 --conf spark.local.dir=/scratch/spark_temp
