import os
from pyspark.sql import SparkSession

# Create path variables
blocks_file = "/processed-data/rusty-dump/blocks.csv"
transactions_file = "/processed-data/rusty-dump/transactions.csv"
tx_out_file = "/processed-data/rusty-dump/tx_in.csv"
tx_in_file = "/processed-data/rusty-dump/tx_out.csv"

# Check if files exist
if not os.path.exists(blocks_file) or not os.path.exists(transactions_file) or not os.path.exists(tx_out_file) or not os.path.exists(tx_in_file):
    print("Some or all csv files do not exist, import cannot begin..")
else:
    print("All csv files available, import to spark begins..")

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Load Bitcoin Blockchain csvdump to Spark SQL") \
        .getOrCreate()

    # Enable compression if needed
    # spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")

    # Set database properties
    spark.sql("SET NAMES ascii COLLATE ascii_bin")
    spark.sql("SET spark.sql.legacy.timeParserPolicy=LEGACY")

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS btc_blockchain")
    spark.sql("USE btc_blockchain")

    # Define table schemas
    blocks_schema = "`id` INT, `hash` BINARY(32), `height` INT, `version` INT, `blocksize` INT, " \
                "`hashPrev` BINARY(32), `hashMerkleRoot` BINARY(32), `nTime` INT, `nBits` INT, `nNonce` INT"

    transactions_schema = "`id` INT, `txid` BINARY(32), `hashBlock` BINARY(32), `version` INT, `lockTime` INT"

    tx_out_schema = "`id` INT, `txid` BINARY(32), `indexOut` INT, `value` BIGINT, `scriptPubKey` BINARY, " \
                "`address` STRING, `unspent` BOOLEAN"

    tx_in_schema = "`id` INT, `txid` BINARY(32), `hashPrevOut` BINARY(32), `indexPrevOut` INT, " \
                "`scriptSig` BINARY, `sequence` INT"

    # Create tables
    spark.sql(f"DROP TABLE IF EXISTS blocks")
    spark.sql(f"CREATE TABLE blocks ({blocks_schema}) USING InnoDB")

    spark.sql(f"DROP TABLE IF EXISTS transactions")
    spark.sql(f"CREATE TABLE transactions ({transactions_schema}) USING InnoDB")

    spark.sql(f"DROP TABLE IF EXISTS tx_out")
    spark.sql(f"CREATE TABLE tx_out ({tx_out_schema}) USING InnoDB")

    spark.sql(f"DROP TABLE IF EXISTS tx_in")
    spark.sql(f"CREATE TABLE tx_in ({tx_in_schema}) USING InnoDB")

    # Set session properties
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("SET spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true")

    # Load data into tables
    spark.sql(f"TRUNCATE TABLE blocks")
    spark.sql(f"LOAD DATA INPATH '{blocks_file}' INTO TABLE blocks OPTIONS (header 'true', delimiter ';')")

    spark.sql(f"TRUNCATE TABLE transactions")
    spark.sql(f"LOAD DATA INPATH '{transactions_file}' INTO TABLE transactions OPTIONS (header 'true', delimiter ';')")

    spark.sql(f"TRUNCATE TABLE tx_out")
    spark.sql(f"LOAD DATA INPATH '{tx_out_file}' INTO TABLE tx_out OPTIONS (header 'true', delimiter ';')")

    spark.sql(f"TRUNCATE TABLE tx_in")
    spark.sql(f"LOAD DATA INPATH '{tx_in_file}' INTO TABLE tx_in OPTIONS (header 'true', delimiter ';')")

    # Add keys
    spark.sql(f"ALTER TABLE blocks ADD CONSTRAINT `pk_blocks` PRIMARY KEY (`id`)")
    spark.sql(f"ALTER TABLE transactions ADD CONSTRAINT `pk_transactions` PRIMARY KEY (`id`)")
    spark.sql(f"ALTER TABLE tx_out ADD CONSTRAINT `pk_tx_out` PRIMARY KEY (`id`)")
    spark.sql(f"ALTER TABLE tx_in ADD CONSTRAINT `pk_tx_in` PRIMARY KEY (`id`)")

    spark.sql(f"CREATE INDEX `idx_txid` ON transactions (`txid`)")
    spark.sql(f"CREATE INDEX `idx_hashPrevOut_indexPrevOut` ON tx_in (`hashPrevOut`, `indexPrevOut`)")
    spark.sql(f"CREATE INDEX `idx_txid_indexOut` ON tx_out (`txid`, `indexOut`)")
    spark.sql(f"CREATE INDEX `idx_address` ON tx_out (`address`)")

    # Flag spent tx outputs
    spark.sql(f"UPDATE tx_out o, tx_in i SET o.unspent = FALSE " \
            f"WHERE o.txid = i.hashPrevOut AND o.indexOut = i.indexPrevOut")

    # Close SparkSession
    spark.stop()

print("Script terminated!")
