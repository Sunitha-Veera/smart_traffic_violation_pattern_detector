from pyspark.sql import SparkSession
from schema import traffic_schema

# Create Spark Session
spark = SparkSession.builder \
    .appName("TrafficDataIngestion") \
    .getOrCreate()

print("📌 Spark Session Created")

# Read CSV using the defined schema
df = spark.read.csv(
    "traffic_data.csv",
    header=True,
    schema=traffic_schema
)

# Show first 10 rows and schema
df.show(10, truncate=False)
df.printSchema()

# Stop Spark session
spark.stop()
