import os
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Load Bitcoin Blockchain unspent to Spark SQL") \
    .getOrCreate()

# paths
block_range = "0-434984"

unspent_file = f"/processed-data/rusty-dump/unspent-{block_range}.csv"
balances_file = f"/processed-data/rusty-dump/balances-{block_range}.csv"
blocks_file = f"/processed-data/rusty-dump/blocks-{block_range}.csv"
transactions_file = f"/processed-data/rusty-dump/transactions-{block_range}.csv"
tx_out_file = f"/processed-data/rusty-dump/tx_in-{block_range}.csv"
tx_in_file = f"/processed-data/rusty-dump/tx_out-{block_range}.csv"

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

# save to database
unspent.saveAsTable("unspent", mode="overwrite")
balances.saveAsTable("balances", mode="overwrite")
blocks.saveAsTable("blocks", mode="overwrite")
transactions.saveAsTable("transactions", mode="overwrite")
tx_out.saveAsTable("tx_out", mode="overwrite")
tx_in.saveAsTable("tx_in", mode="overwrite")

# add keys
spark.sql(f"ALTER TABLE blocks ADD CONSTRAINT `pk_blocks` PRIMARY KEY (`id`)")
spark.sql(f"ALTER TABLE transactions ADD CONSTRAINT `pk_transactions` PRIMARY KEY (`id`)")
spark.sql(f"ALTER TABLE tx_out ADD CONSTRAINT `pk_tx_out` PRIMARY KEY (`id`)")
spark.sql(f"ALTER TABLE tx_in ADD CONSTRAINT `pk_tx_in` PRIMARY KEY (`id`)")

spark.sql(f"CREATE INDEX `idx_txid` ON transactions (`txid`)")
spark.sql(f"CREATE INDEX `idx_hashPrevOut_indexPrevOut` ON tx_in (`hashPrevOut`, `indexPrevOut`)")
spark.sql(f"CREATE INDEX `idx_txid_indexOut` ON tx_out (`txid`, `indexOut`)")
spark.sql(f"CREATE INDEX `idx_address` ON tx_out (`address`)")

# flag spent tx outputs
spark.sql(f"UPDATE tx_out o, tx_in i SET o.unspent = FALSE " \
        f"WHERE o.txid = i.hashPrevOut AND o.indexOut = i.indexPrevOut")
