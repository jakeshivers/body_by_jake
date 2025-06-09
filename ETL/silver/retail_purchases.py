from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("RetailPurchasesSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load from Bronze
df = spark.read.format("delta").load("output/retail_purchases_bronze")

# Silver transformation
df_silver = df \
    .dropDuplicates(["member_id", "timestamp", "product_name"]) \
    .filter("price > 0") \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp"))

# Write to Silver
df_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("product_name", "year", "month") \
    .save("output/retail_purchases_silver")

print("✅ Saved retail_purchases_silver")
spark.stop()
