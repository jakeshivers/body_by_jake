from pyspark.sql.functions import current_timestamp, expr, floor, rand, expr, current_date, floor, rand, date_sub, col, expr
from pyspark.sql import SparkSession
import random
from delta import configure_spark_with_delta_pip





builder = SparkSession.builder \
    .appName("ClassCheckins") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Number of fake records to generate
N = 20_000_000
class_list = ["Yoga", "Spin", "Stretch", "Peleton", "Aerobics"]

# Generate string: "'Yoga','Spin','Stretch','Peleton','Aerobics'"
class_expr_values = ",".join([f"'{c}'" for c in class_list])

df = spark.range(1, N + 1).toDF("checkin_id") \
    .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
    .withColumn("class_name", expr(f"element_at(array({class_expr_values}), int(rand() * {len(class_list)}) + 1)")) \
    .withColumn("duration_mins", (rand() * 60 + 20).cast("int")) \

# Create a randomized offset in days
df = df.withColumn("days_offset", (floor(rand() * 365)).cast("int"))

# Use PySpark's built-in date_sub to subtract that from today
df = df.withColumn("timestamp", date_sub(current_date(), col("days_offset")))

# Write to Delta (partitioned by class_name)
df.write.format("delta").mode("overwrite").partitionBy("class_name").save("output/class_checkins_bronze")

print("✅ Saved 20M class check-ins to Delta (partitioned)")
spark.stop()
