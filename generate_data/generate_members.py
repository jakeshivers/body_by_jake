from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, rand, expr, current_date, date_sub
from faker import Faker
from delta import configure_spark_with_delta_pip

# Spark setup
builder = SparkSession.builder \
    .appName("Members") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

fake = Faker()
N = 1_000_000
plans = ["Basic", "Premium", "Elite", "Student", "Family"]
plan_expr = ",".join([f"'{p}'" for p in plans])

# Generate 1M members
df = spark.range(1, N + 1).toDF("member_id") \
    .withColumn("age", (rand() * 40 + 18).cast("int")) \
    .withColumn("signup_offset", (floor(rand() * 365)).cast("int")) \
    .withColumn("signup_date", date_sub(current_date(), col("signup_offset"))) \
    .withColumn("zipcode", expr("CAST(FLOOR(rand() * 90000 + 10000) AS STRING)")) \
    .withColumn("plan", expr(f"element_at(array({plan_expr}), int(rand() * {len(plans)}) + 1)"))

# Add fake names with a UDF (slow, but adds realism)
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(StringType())
def fake_name():
    return fake.name()

df = df.withColumn("name", fake_name())

# Save as Delta, partitioned by plan
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("plan") \
    .save("output/members_bronze")

print("✅ Saved 1M member records to Delta (partitioned by plan)")
spark.stop()