FILE_PATH = "/data/2010_2011/output/transactions/start_block=00043790/end_block=00043889/transactions_00043790_00043889.json"
df = spark.read.json(FILE_PATH)
df.printSchema()

FILE_PATH = f"/data/2010_2011/output/transactions/start_block=00043790/end_block=00043889/transactions_00043790_00043889.json"
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW transactions USING json OPTIONS (path '{FILE_PATH}')")
spark.sql("SELECT * FROM transactions LIMIT 10").show()