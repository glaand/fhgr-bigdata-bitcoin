# To use this script just paste it to the pyspark shell and run the code
# Or execute like this in bash: spark-submit create_btc_prices_tables.py

import os
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create SparkSession
# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

hourly_prices_file = f"/btc_to_usd/prices_hourly.csv"
daily_prices_file = f"/btc_to_usd/prices_daily.csv"

# check if files exist
if os.path.exists(hourly_prices_file) and os.path.exists(daily_prices_file):
    print("All files exist")

# create dfs
hourly_prices=spark.read.option("delimiter", ",").option("header", True).csv(hourly_prices_file)
daily_prices=spark.read.option("delimiter", ",").option("header", True).csv(daily_prices_file)

# create database
spark.sql("CREATE SCHEMA IF NOT EXISTS btc_blockchain")
spark.sql("USE btc_blockchain")

# create Hive internal table
hourly_prices.write.mode('overwrite').saveAsTable("hourly_prices")
daily_prices.write.mode('overwrite').saveAsTable("daily_prices")

# read dfs from Hive tables (Can also be used after successful import to load data to df)
hourly_prices_df=spark.read.table("hourly_prices")
daily_prices_df=spark.read.table("daily_prices")
