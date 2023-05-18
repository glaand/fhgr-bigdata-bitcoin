import os
from pyspark.sql import SparkSession

balances_file = "/processed-data/rusty-dump/unspent.csv"

# Check if file exists
if not os.path.exists(balances_file):
    print("Csv file does not exist, import cannot begin..")
else:
    print("Csv file available, import to spark begins..")

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Load Bitcoin Blockchain balances to Spark SQL") \
        .getOrCreate()

    # Enable compression if needed
    # spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")

    # Set database properties
    spark.sql("SET NAMES ascii COLLATE ascii_bin")
    spark.sql("SET spark.sql.legacy.timeParserPolicy=LEGACY")

    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS btc_blockchain")
    spark.sql("USE btc_blockchain")

    # Define balances table schema
    balances_schema = "`address` STRING, `balance` BIGINT"

    # Create balances table
    spark.sql("DROP TABLE IF EXISTS balances")
    spark.sql(f"CREATE TABLE balances ({balances_schema}) USING InnoDB")

    # Set session properties
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("SET spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true")

    # Load data into balances table
    spark.sql(f"TRUNCATE TABLE balances")
    spark.sql(f"LOAD DATA INPATH '{balances_file}' INTO TABLE balances OPTIONS (header 'true', delimiter ';')")

    # Close SparkSession
    spark.stop()

print("Script terminated!")
