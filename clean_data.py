from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from schema import traffic_schema

# ---------------------------
# Create Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("TrafficDataCleaning") \
    .getOrCreate()

# ---------------------------
# Load Raw Data
# ---------------------------
df = spark.read.csv("traffic_data.csv", header=True, schema=traffic_schema)

print("RAW DATA BEFORE CLEANING:")
df.show(5, truncate=False)

# ---------------------------
# STEP 1: Handle Missing Values
# ---------------------------

# Fill missing severity with "Unknown"
df = df.fillna({"Severity": "Unknown"})

# Replace empty timestamps with null
df = df.withColumn(
    "Timestamp",
    when(col("Timestamp") == "", None).otherwise(col("Timestamp"))
)

# ---------------------------
# STEP 2: Fix Malformed Timestamps
# ---------------------------
df = df.withColumn(
    "Timestamp_clean",
    to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# Drop rows where timestamp conversion failed
df = df.filter(col("Timestamp_clean").isNotNull())

# ---------------------------
# STEP 3: Validate Categorical Fields
# ---------------------------

valid_violation_types = ["Speeding", "Red Light", "Parking", "Wrong Lane", "Overspeeding", "Signal Jump", "No Helmet", "Seat Belt Violation", "Wrong Parking"]

df = df.filter(col("ViolationType").isin(valid_violation_types))

# ---------------------------
# STEP 4: Optional: Validate Coordinates
# ---------------------------
df = df.filter(
    (col("Latitude").between(-90, 90)) &
    (col("Longitude").between(-180, 180))
)

# ---------------------------
# STEP 5: Write Clean Data as Parquet
# ---------------------------
df.write.mode("overwrite").parquet("cleaned/traffic_cleaned.parquet")

print("\nCLEANING COMPLETE. PARQUET FILE SAVED.")

# Stop Spark
spark.stop()
