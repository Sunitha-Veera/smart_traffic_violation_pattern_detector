from pyspark.sql.types import StructType, StructField, StringType, DoubleType

traffic_schema = StructType([
    StructField("ViolationID", StringType(), True),
    StructField("VehicleID", StringType(), True),
    StructField("ViolationType", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("LocationID", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])
