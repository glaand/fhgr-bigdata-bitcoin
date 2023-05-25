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

daily_transactions_value = spark.sql("""
    SELECT 
        COUNT(*),
        YEAR(blocks.ntime), 
        MONTH(blocks.ntime), 
        DAY(blocks.ntime)
    FROM tx_out
    JOIN transactions ON transactions.txid = tx_out.txid
    JOIN blocks b ON b.hash = transactions.blockHash
    WHERE SUBSTRING(tx_out.script, 1, 2) = '6a'
    GROUP BY YEAR(blocks.ntime), MONTH(blocks.ntime), DAY(blocks.ntime)
""")
daily_transactions_value.write.csv("/reduced_data/opreturn_transactions.csv")
