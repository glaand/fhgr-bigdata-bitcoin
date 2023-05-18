FILE_PATH = "/data/prices_hourly.csv"
df = spark.read.csv(FILE_PATH)
df.printSchema()

spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW prices_hourly USING csv OPTIONS (path '{FILE_PATH}')")
spark.sql("SELECT * FROM prices_hourly LIMIT 10").show()
