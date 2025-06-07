from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import os

def get_spark(app_name="partition_test"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-core_2.12:2.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.1",
            "org.apache.hadoop:hadoop-common:3.3.1",
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
        ]))
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = get_spark()

    df = spark.createDataFrame([
        (1, "Jake"),
        (2, "Skylar")
    ], ["id", "name"]).withColumn("checkin_date", current_date())

    df.show()

    print("Writing to Delta at s3a://bbj-lakehouse/bronze/test_partitioned ...")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("checkin_date") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .save("s3a://bbj-lakehouse/bronze/test_partitioned")

    print("✅ Write successful.")
    spark.stop()
