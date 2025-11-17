from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    hour, dayofweek, month, year, col, count
)

# ----------------------------------------------------------
# 1. Start Spark Session
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("TrafficViolationPatternAnalysis") \
    .getOrCreate()

print("\n📌 Loading cleaned parquet data...")
df = spark.read.parquet("cleaned/traffic_cleaned.parquet")
df.show(5, truncate=False)

# ----------------------------------------------------------
# WEEK 3 — Derive Time-Based Features
# ----------------------------------------------------------
print("\n📌 Adding time-based features...")

df_time = df.withColumn("Hour", hour("Timestamp_clean")) \
            .withColumn("DayOfWeek", dayofweek("Timestamp_clean")) \
            .withColumn("Month", month("Timestamp_clean")) \
            .withColumn("Year", year("Timestamp_clean"))

df_time.show(5, truncate=False)

# ----------------------------------------------------------
# WEEK 3 — Aggregations
# ----------------------------------------------------------

# 1️⃣ Violations per hour
violations_per_hour = df_time.groupBy("Hour") \
    .agg(count("*").alias("Total_Violations"))

# 2️⃣ Violations per day of week
violations_per_day = df_time.groupBy("DayOfWeek") \
    .agg(count("*").alias("Total_Violations"))

# 3️⃣ Violations by offense type
violations_by_type = df_time.groupBy("ViolationType") \
    .agg(count("*").alias("Total_Violations"))

# 4️⃣ Crosstab: Violation Type × Hour
cross_tab = df_time.crosstab("ViolationType", "Hour")

# ----------------------------------------------------------
# WEEK 4 — Location-Based Analysis
# ----------------------------------------------------------

# 1️⃣ Total violations per location
violations_per_location = df_time.groupBy("LocationID") \
    .agg(count("*").alias("Total_Violations"))

# 2️⃣ Top 5 locations with highest violations
top_locations = violations_per_location.orderBy(
    col("Total_Violations").desc()
).limit(5)

# ----------------------------------------------------------
# Save all results as Parquet files
# ----------------------------------------------------------
output_path = "aggregated_results/"

violations_per_hour.write.mode("overwrite").parquet(output_path + "violations_per_hour")
violations_per_day.write.mode("overwrite").parquet(output_path + "violations_per_day")
violations_by_type.write.mode("overwrite").parquet(output_path + "violations_by_type")
cross_tab.write.mode("overwrite").parquet(output_path + "cross_tab_type_hour")
violations_per_location.write.mode("overwrite").parquet(output_path + "violations_per_location")
top_locations.write.mode("overwrite").parquet(output_path + "top_locations")

print("\n🎉 Aggregation Complete!")
print("Results saved in 'aggregated_results/' folder.")

spark.stop()
