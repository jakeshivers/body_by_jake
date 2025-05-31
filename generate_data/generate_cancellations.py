from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, rand, expr, current_date, date_sub
from delta import configure_spark_with_delta_pip

# Spark setup
builder = SparkSession.builder \
    .appName("Cancellations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Assume 75K total cancellations
N = 75_000
reasons = [
    "Too expensive",
    "Not using enough",
    "Moved",
    "Found another gym",
    "No longer interested",
    "Medical reasons",
    "Poor facility experience"
]

reason_expr = ",".join([f"'{r}'" for r in reasons])

df = spark.range(1, N + 1).toDF("cancel_id") \
    .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
    .withColumn("days_offset", (floor(rand() * 365)).cast("int")) \
    .withColumn("timestamp", date_sub(current_date(), col("days_offset"))) \
    .withColumn("reason", expr(f"element_at(array({reason_expr}), int(rand() * {len(reasons)}) + 1)"))

# Save to Delta (no partitioning needed — it's small)
df.write.format("delta") \
    .mode("overwrite") \
    .save("output/cancellations_bronze")

print("✅ Saved 75K cancellation records to Delta")
spark.stop()
