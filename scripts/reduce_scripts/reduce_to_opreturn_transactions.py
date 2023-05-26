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
        YEAR(blocks.ntime), 
        MONTH(blocks.ntime), 
        DAY(blocks.ntime)
    FROM tx_out_temp
    JOIN transactions ON transactions.txid = tx_out_temp.txid
    JOIN blocks b ON b.hash = transactions.blockHash
    WHERE SUBSTRING(tx_out_temp.script, 1, 2) = '6a'
    GROUP BY YEAR(blocks.ntime), MONTH(blocks.ntime), DAY(blocks.ntime)
""")
daily_transactions_value.show()
daily_transactions_value.write.csv("/reduced_data/opreturn_transactions.csv")
