# To use this script just paste it to the pyspark shell and run the code
# Or execute like this in bash: spark-submit create_dfs_and_database.py

import os
from os.path import abspath
from pyspark.sql import SparkSession

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
tx_out_file = f"/processed-data/rusty-dump/tx_in-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers
tx_in_file = f"/processed-data/rusty-dump/tx_out-{block_range}.csv"  # this files need to be generated with the provided shell script because the originial file doesent contain headers

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

spark.sql(f"CREATE INDEX `idx_txid` ON transactions (`txid`)")
spark.sql(f"CREATE INDEX `idx_hashPrevOut_indexPrevOut` ON tx_in (`hashPrevOut`, `indexPrevOut`)")
spark.sql(f"CREATE INDEX `idx_txid_indexOut` ON tx_out (`txid`, `indexOut`)")
spark.sql(f"CREATE INDEX `idx_address` ON tx_out (`address`)")

# flag spent tx outputs
spark.sql(f"UPDATE tx_out o, tx_in i SET o.unspent = FALSE " \
        f"WHERE o.txid = i.hashPrevOut AND o.indexOut = i.indexPrevOut")

# read dfs from Hive tables
unspent=spark.read.table("unspent")
balances=spark.read.table("balances")
blocks=spark.read.table("blocks")
transactions=spark.read.table("transactions")
tx_out=spark.read.table("tx_out")
tx_in=spark.read.table("tx_in")