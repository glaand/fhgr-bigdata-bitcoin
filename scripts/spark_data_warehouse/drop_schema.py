import os
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("DROP btc_blockchain") \
    .getOrCreate()

spark.sql("DROP SCHEMA btc_blockchain CASCADE")