# To use this script just paste it to the pyspark shell and run the code
# Or execute like this in bash: spark-submit create_dfs_and_database.py

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

# paths
block_range = "0-790000"

#unspent_file = f"/processed-data/rusty-dump/unspent-{block_range}.csv"
#balances_file = f"/processed-data/rusty-dump/balances-{block_range}.csv"
blocks_file = f"/processed-data/rusty-dump/blocks-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
transactions_file = f"/processed-data/rusty-dump/transactions-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
tx_out_file = f"/processed-data/rusty-dump/tx_out-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
tx_in_file = f"/processed-data/rusty-dump/tx_in-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers

# check if files exist
#if os.path.exists(unspent_file) and os.path.exists(balances_file) and os.path.exists(blocks_file) and os.path.exists(transactions_file) and os.path.exists(tx_out_file) and os.path.exists(tx_in_file):
#    print("All files exist")

# create dfs
#unspent=spark.read.option("delimiter", ";").option("header", True).csv(unspent_file)
#balances=spark.read.option("delimiter", ";").option("header", True).csv(balances_file)
blocks=spark.read.option("delimiter", ";").option("header", True).csv(blocks_file)
transactions=spark.read.option("delimiter", ";").option("header", True).csv(transactions_file)
tx_out=spark.read.option("delimiter", ";").option("header", True).csv(tx_out_file)
tx_in=spark.read.option("delimiter", ";").option("header", True).csv(tx_in_file)

# create database
spark.sql("CREATE SCHEMA IF NOT EXISTS btc_blockchain")
spark.sql("USE btc_blockchain")

# create Hive internal table
#unspent.write.mode('overwrite').saveAsTable("unspent")
#balances.write.mode('overwrite').saveAsTable("balances")
blocks.write.mode('overwrite').saveAsTable("blocks")
transactions.write.mode('overwrite').saveAsTable("transactions")
tx_out.write.mode('overwrite').saveAsTable("tx_out_temp")
tx_in.write.mode('overwrite').saveAsTable("tx_in")
  
# read dfs from Hive tables (Can also be used after successful import to load data to df)
#unspent_df=spark.read.table("unspent")
#balances_df=spark.read.table("balances")
blocks_df=spark.read.table("blocks")
transactions_df=spark.read.table("transactions")
tx_out_temp_df=spark.read.table("tx_out_temp")
tx_in_df=spark.read.table("tx_in")

# Create unspent column directly in df (wont be saved in database)
# Join the 'tx_out' and 'tx_in' DataFrames based on the conditions

# tx_out_updated_df = tx_out_temp_df.alias('o').join(
#     tx_in_df.alias('i'),
#     (col('o.txid') == col('i.hashPrevOut')) & (col('o.indexOut') == col('i.indexPrevOut')),
#     'left_outer'
# ).select(
#     'o.txid',
#     'o.indexOut',
#     #'o.height',  # maybe not needed because info not in csv
#     'o.value',
#     'o.scriptPubKey',
#     'o.address',
#     when(col('i.txid').isNull(), True).otherwise(False).alias('unspent')
# )

# save table permanent and load to df again
# tx_out_updated_df.write.mode('overwrite').saveAsTable("tx_out")
# tx_out_df=spark.read.table("tx_out")
