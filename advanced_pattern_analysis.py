from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, floor, when,
    count, avg, stddev, round
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import os

# ============================================
#  Spark Setup
# ============================================
spark = SparkSession.builder \
    .appName("Advanced Pattern Analysis - Milestone 3") \
    .getOrCreate()

print("\n📌 Loading cleaned dataset...")
df = spark.read.parquet("cleaned/traffic_cleaned.parquet")

df.show(5, truncate=False)

# ============================================
#  Week 5 — DETAILED TIME-BASED FEATURES
# ============================================
print("\n📌 Adding granular time-based windows...")

df = df.withColumn("Hour", hour(col("Timestamp_clean"))) \
       .withColumn("DayOfWeek", dayofweek(col("Timestamp_clean"))) \
       .withColumn("Month", month(col("Timestamp_clean"))) \
       .withColumn("Year", year(col("Timestamp_clean"))) \
       .withColumn("ThreeHourWindow", floor(col("Hour") / 3)) \
       .withColumn("Weekend", when(col("DayOfWeek").isin(1, 7), 1).otherwise(0))

df.show(5, truncate=False)

# ============================================
#  Time Window Aggregations
# ============================================
print("\n📌 Aggregating violations by 3-hour windows...")
violations_by_3hr = df.groupBy("ThreeHourWindow").count()
violations_by_3hr.show()

print("\n📌 Correlation between violation type and peak time windows...")
type_vs_time = df.groupBy("ViolationType", "ThreeHourWindow").count()
type_vs_time.show(10)

# ============================================
#  Week 5 — SPATIAL GROUPING (Lat/Long present)
# ============================================
print("\n📌 Checking location format (Lat/Long grouping)...")

if "Latitude" in df.columns and "Longitude" in df.columns:
    print("📍 Coordinates detected — Performing spatial grouping...")

    # Grid cell grouping (round latitude/longitude)
    df = df.withColumn("LatGrid", round(col("Latitude"), 2)) \
           .withColumn("LonGrid", round(col("Longitude"), 2))

    hotspot_grid = df.groupBy("LatGrid", "LonGrid").count()
    hotspot_grid.show(10)
else:
    print("⚠ No coordinate data found — Skipping spatial grid grouping.")
    hotspot_grid = None

# ============================================
#  Week 6 — STATISTICAL HOTSPOTS
# ============================================
print("\n📌 Calculating statistically significant hotspots...")

if hotspot_grid:
    stats = hotspot_grid.agg(avg("count").alias("mean"), stddev("count").alias("std")).first()
    mean_val = stats["mean"]
    std_val = stats["std"]

    hotspot_zones = hotspot_grid.withColumn(
        "ZScore", (col("count") - mean_val) / std_val
    ).filter(col("ZScore") > 2)

    print("\n🔥 Significant Hotspots (Z-score > 2)")
    hotspot_zones.show(10)
else:
    hotspot_zones = None

# ============================================
#  Week 6 — K-MEANS CLUSTERING
# ============================================
print("\n📌 Running K-Means clustering on coordinates...")

if hotspot_grid:
    assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol="features")
    vector_df = assembler.transform(df.dropna(subset=["Latitude", "Longitude"]))

    kmeans = KMeans(k=4, seed=1)
    model = kmeans.fit(vector_df)
    clustered = model.transform(vector_df)

    print("\n📌 K-Means cluster centers:")
    for c in model.clusterCenters():
        print(c)

    clustered.show(10)
else:
    print("⚠ Cannot cluster — coordinate data not available.")

# ============================================
#  OUTPUT STORAGE
# ============================================
print("\n📦 Saving results to 'hotspot_results/' ...")
os.makedirs("hotspot_results", exist_ok=True)

violations_by_3hr.write.mode("overwrite").parquet("hotspot_results/violations_by_3hour.parquet")
type_vs_time.write.mode("overwrite").parquet("hotspot_results/type_vs_time.parquet")

if hotspot_grid:
    hotspot_grid.write.mode("overwrite").parquet("hotspot_results/spatial_grid_counts.parquet")
if hotspot_zones:
    hotspot_zones.write.mode("overwrite").parquet("hotspot_results/statistical_hotspots.parquet")
if hotspot_grid:
    clustered.write.mode("overwrite").parquet("hotspot_results/kmeans_clusters.parquet")

print("\n🎉 Milestone 3 Completed Successfully!")
print("➡ Outputs saved to: hotspot_results/")

spark.stop()
