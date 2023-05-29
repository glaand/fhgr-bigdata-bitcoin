import os
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# select db
spark.sql("USE btc_blockchain")
daily_transactions_value = spark.sql("""
    SELECT 
        COUNT(*),
        b.year, 
        b.month, 
        b.day
    FROM tx_out_temp
    JOIN transactions ON transactions.txid = tx_out_temp.txid
    JOIN blocks b ON b.hash = transactions.hashBlock
    WHERE SUBSTRING(tx_out_temp.scriptPubKey, 1, 2) = '6a'
    GROUP BY b.year, b.month, b.day
""")
daily_transactions_value.write.csv("/reduced-data/opreturn_transactions.csv")
