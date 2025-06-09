from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .appName("GenTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", ",".join([
        "/home/jake/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar",
        "/home/jake/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar",
        "/home/jake/.ivy2/jars/io.delta_delta-spark_2.12-3.3.2.jar",
        "/home/jake/.ivy2/jars/io.delta_delta-storage-3.3.2.jar"  # ✅ Add this
    ])) \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Sample DataFrame
data = [(1, "Jake"), (2, "Skylar"), (3, "Chris")]
df = spark.createDataFrame(data, ["member_id", "name"])

df.show()
print(f"Count: {df.count()}")


try:
    df.write.format("delta").mode("overwrite").save("s3a://bbj-lakehouse/bronze/members")
    print("✅ Write succeeded")
except Exception as e:
    print(f"❌ Write failed: {e}")
