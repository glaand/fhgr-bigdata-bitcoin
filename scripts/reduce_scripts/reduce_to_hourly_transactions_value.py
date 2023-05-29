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

hourly_transactions_value = spark.sql("""
    SELECT 
        SUM(tx_out_temp.value), 
        h.year, 
        h.month, 
        h.day, 
        h.hour, 
        MAX(h.avg)
    FROM tx_out_temp
    JOIN transactions ON transactions.txid = tx_out_temp.txid
    JOIN blocks b ON b.hash = transactions.hashBlock
    JOIN hourly_prices h 
        ON h.year = b.year
        AND h.month = b.month
        AND h.day = b.day
        AND h.hour = b.hour
    GROUP BY h.year, h.month, h.day, h.hour
""")
hourly_transactions_value.write.csv("/reduced-data/hourly_transactions_value.csv")
