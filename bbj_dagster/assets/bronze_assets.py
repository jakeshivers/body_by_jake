from dagster import asset
from src.spark_session import get_spark
from generate_data.generate_members import generate_members_df
from generate_data.generate_checkins import generate_checkins_df
from generate_data.generate_facility_usage import generate_facility_usage_df
from generate_data.generate_cancellations import generate_cancellations_df
from generate_data.generate_retail import generate_retail_df

BRONZE_PATH = "s3a://bbj-lakehouse/bronze"

def write_partitioned_delta(df, path, partition_by=None):
    writer = df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(path)

@asset
def members_bronze():
    spark = get_spark("bronze_members")
    df = generate_members_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/members", partition_by="plan")
    spark.stop()

@asset
def checkins_bronze():
    spark = get_spark("bronze_checkins")
    df = generate_checkins_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/checkins", partition_by="class_name")
    spark.stop()

@asset
def facility_usage_bronze():
    spark = get_spark("facility_usage")
    df = generate_facility_usage_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/facility_usage", partition_by="facility")
    spark.stop()

@asset
def cancellations_bronze():
    spark = get_spark("cancellations")
    df = generate_cancellations_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/cancellations", partition_by="reason")
    spark.stop()

@asset
def retail_bronze():
    spark = get_spark("retail")
    df = generate_retail_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/retail", partition_by="product_name")
    spark.stop()
