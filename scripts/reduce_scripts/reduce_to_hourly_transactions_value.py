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
        SUM(tx_out.value), 
        transactions.txid, 
        YEAR(hourly_prices.time), 
        MONTH(hourly_prices.time), 
        DAY(hourly_prices.time), 
        HOUR(hourly_prices.time), 
        MAX((hourly_prices.high+hourly_prices.low)/2) as price
    FROM tx_out
    JOIN transactions ON transactions.txid = tx_out.txid
    JOIN blocks b ON b.hash = transactions.blockHash
    JOIN hourly_prices h 
        ON h.year = YEAR(date_from_unix_date(b.ntime))
        ON h.month = MONTH(date_from_unix_date(b.ntime))
        ON h.day = DAY(date_from_unix_date(b.ntime))
        ON h.hour = HOUR(date_from_unix_date(b.ntime))
    GROUP BY transactions.txid, h.year, h.month, h.day, h.hour
""")
hourly_transactions_value.show()
hourly_transactions_value.write.csv("/reduced_data/hourly_transactions_value.csv")
