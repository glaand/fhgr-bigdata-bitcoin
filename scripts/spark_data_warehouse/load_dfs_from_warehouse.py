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

# select db
spark.sql("USE btc_blockchain")

# read dfs from Hive tables (Can also be used after successful import to load data to df)
unspent_df=spark.read.table("unspent")
balances_df=spark.read.table("balances")
blocks_df=spark.read.table("blocks")
transactions_df=spark.read.table("transactions")
tx_out_df=spark.read.table("tx_out")
tx_in_df=spark.read.table("tx_in")

# Have fun with dataframes
