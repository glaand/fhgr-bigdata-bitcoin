import os
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the schema.sql file
with open(os.path.join("sql", "schema.sql"), "r") as f:
    schema_sql = f.read()

# Execute the entire schema.sql file as a single SQL statement
spark.sql(schema_sql)

# Close the SparkSession
spark.stop()