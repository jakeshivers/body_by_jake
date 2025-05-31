from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, rand, expr, current_date, date_sub
from delta import configure_spark_with_delta_pip

# Spark config
builder = SparkSession.builder \
    .appName("FacilityUsage") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Generate N rows
N = 12_000_000
facilities = ["Sauna", "Pool", "Steam Room", "Ice Bath"]

facility_expr = ",".join([f"'{f}'" for f in facilities])

df = spark.range(1, N + 1).toDF("usage_id") \
    .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
    .withColumn("duration_mins", (rand() * 80 + 10).cast("int")) \
    .withColumn("days_offset", (floor(rand() * 365)).cast("int")) \
    .withColumn("timestamp", date_sub(current_date(), col("days_offset"))) \
    .withColumn("facility", expr(f"element_at(array({facility_expr}), int(rand() * {len(facilities)}) + 1)"))

# Write to Delta
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("facility") \
    .save("output/facility_usage_bronze")

print("✅ Saved 12M facility usage records to Delta (partitioned by facility)")
spark.stop()
