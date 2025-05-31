from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ✅ Properly configure Spark for Delta Lake 3.x
builder = SparkSession.builder \
    .appName("HelloDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# ✅ Pass to Delta configurator
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Optional: Reduce log noise
spark.sparkContext.setLogLevel("ERROR")

# ✅ Load CSV
df = spark.read.csv("members.csv", header=True, inferSchema=True)
print("👀 Original CSV:")
df.show()
df.printSchema()

# ✅ Save as Parquet
df.write.mode("overwrite").parquet("output/members_parquet")
print("📦 Saved to Parquet")

# ✅ Load from Parquet
parquet_df = spark.read.parquet("output/members_parquet")
print("🔁 Read from Parquet:")
parquet_df.show()

# ✅ Save as Delta
df.write.format("delta").mode("overwrite").save("output/members_delta")
print("📦 Saved to Delta")

# ✅ Load from Delta
delta_df = spark.read.format("delta").load("output/members_delta")
print("🔁 Read from Delta:")
delta_df.show()

# ✅ Stop Spark
spark.stop()
