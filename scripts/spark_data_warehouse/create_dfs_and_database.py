# To use this script just paste it to the pyspark shell and run the code
# Or execute like this in bash: spark-submit create_dfs_and_database.py

import os
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# paths
block_range = "0-434984"

unspent_file = f"/processed-data/rusty-dump/unspent-{block_range}.csv"
balances_file = f"/processed-data/rusty-dump/balances-{block_range}.csv"
blocks_file = f"/processed-data/rusty-dump/blocks-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
transactions_file = f"/processed-data/rusty-dump/transactions-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
tx_out_file = f"/processed-data/rusty-dump/tx_out-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
tx_in_file = f"/processed-data/rusty-dump/tx_in-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers

# check if files exist
if os.path.exists(unspent_file) and os.path.exists(balances_file) and os.path.exists(blocks_file) and os.path.exists(transactions_file) and os.path.exists(tx_out_file) and os.path.exists(tx_in_file):
    print("All files exist")

# create dfs
unspent=spark.read.option("delimiter", ";").option("header", True).csv(unspent_file)
balances=spark.read.option("delimiter", ";").option("header", True).csv(balances_file)
blocks=spark.read.option("delimiter", ";").option("header", True).csv(blocks_file)
transactions=spark.read.option("delimiter", ";").option("header", True).csv(transactions_file)
tx_out=spark.read.option("delimiter", ";").option("header", True).csv(tx_out_file)
tx_in=spark.read.option("delimiter", ";").option("header", True).csv(tx_in_file)

# create database
spark.sql("CREATE SCHEMA IF NOT EXISTS btc_blockchain")
spark.sql("USE btc_blockchain")

# create Hive internal table
unspent.write.mode('overwrite').saveAsTable("unspent")
balances.write.mode('overwrite').saveAsTable("balances")
blocks.write.mode('overwrite').saveAsTable("blocks")
transactions.write.mode('overwrite').saveAsTable("transactions")
tx_out.write.mode('overwrite').saveAsTable("tx_out")
tx_in.write.mode('overwrite').saveAsTable("tx_in")
  
# read dfs from Hive tables (Can also be used after successful import to load data to df)
unspent_df=spark.read.table("unspent")
balances_df=spark.read.table("balances")
blocks_df=spark.read.table("blocks")
transactions_df=spark.read.table("transactions")
tx_out_df=spark.read.table("tx_out")
tx_in_df=spark.read.table("tx_in")

# Join the 'tx_out' and 'tx_in' DataFrames based on the conditions
joined_df = tx_out_df.join(tx_in_df, (tx_out_df.txid == tx_in_df.hashPrevOut) & (tx_out_df.indexOut == tx_in_df.indexPrevOut), "inner")

# Update the 'unspent' column to FALSE
updated_df = joined_df.withColumn("unspent", col("unspent").cast("boolean").otherwise(False))

# Select the required columns and overwrite the 'tx_out' DataFrame
tx_out_df = updated_df.select("txid", "hashPrevOut", "indexPrevOut", "scriptSig", "sequence", "unspent")
