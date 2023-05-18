import os
from pyspark.sql import SparkSession

unspent_file = "/processed-data/rusty-dump/unspent.csv"

# Check if file exists
if not os.path.exists(unspent_file):
    print("Csv file does not exist, import cannot begin..")
else:
    print("Csv file available, import to spark begins..")

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Load Bitcoin Blockchain unspentcsvdump to Spark SQL") \
        .getOrCreate()

    # Enable compression if needed
    # spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")

    # Set database properties
    spark.sql("SET NAMES ascii COLLATE ascii_bin")
    spark.sql("SET spark.sql.legacy.timeParserPolicy=LEGACY")

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS btc_blockchain")
    spark.sql("USE btc_blockchain")

    # Define unspent table schema
    unspent_schema = "`txid` BINARY(32), `indexOut` INT, `height` INT, `value` BIGINT, `address` STRING"

    # Create unspent table
    spark.sql("DROP TABLE IF EXISTS unspent")
    spark.sql(f"CREATE TABLE unspent ({unspent_schema}) USING InnoDB")

    # Set session properties
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("SET spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true")

    # Load data into unspent table
    spark.sql(f"TRUNCATE TABLE unspent")
    spark.sql(f"LOAD DATA INPATH '{unspent_file}' INTO TABLE unspent OPTIONS (header 'true', delimiter ';')")

    # Close SparkSession
    spark.stop()

print("Script terminated!")
