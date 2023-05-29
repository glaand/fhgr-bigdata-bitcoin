import os
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

hourly_transactions_folder = '/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data/hourly_transactions_value.csv'
opreturn_transactions_folder = '/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data/opreturn_transactions.csv'
hourly_transactions_file = '/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data/file_hourly_transactions_value.csv'
opreturn_transactions_file = '/scratch/fhgr-bigdata-bitcoin/mounted-data/reduced-data/file_opreturn_transactions.csv'

hourly_transactions_schema = StructType([\
    StructField("sum_satoshis", FloatType(), True),\
    StructField("year", IntegerType(), True),\
    StructField("month", IntegerType(), True),\
    StructField("day", IntegerType(), True),\
    StructField("hour", IntegerType(), True),\
    StructField("btc_usd_avg_price", FloatType(), True)])

hourly_transactions = spark.read.option("delimiter", ",") \
                        .schema(hourly_transactions_schema) \
                        .option("header", False) \
                        .csv(hourly_transactions_folder)

hourly_transactions.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(hourly_transactions_file)

opreturn_transactions_schema = StructType([\
    StructField("cnt", IntegerType(), True),\
    StructField("year", IntegerType(), True),\
    StructField("month", IntegerType(), True),\
    StructField("day", IntegerType(), True)])
    
opreturn_transactions = spark.read.option("delimiter", ",") \
                        .schema(opreturn_transactions_schema) \
                        .option("header", False) \
                        .csv(opreturn_transactions_folder)

opreturn_transactions.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(opreturn_transactions_file)