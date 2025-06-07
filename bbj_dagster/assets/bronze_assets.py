import os
from dagster import asset, AssetMaterialization, DailyPartitionsDefinition, Output
from pyspark.sql.functions import to_date, col
from src.spark_session import get_spark
from src.write_delta import write_partitioned_delta
from generate_data.generate_members import generate_members_df
from generate_data.generate_checkins import generate_checkins_df
from generate_data.generate_facility_usage import generate_facility_usage_df
from generate_data.generate_cancellations import generate_cancellations_df
from generate_data.generate_retail import generate_retail_df

DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2025-06-01")
BRONZE_PATH = "s3a://bbj-lakehouse/bronze"

@asset(partitions_def=DAILY_PARTITIONS)
def checkins_bronze():
    spark = get_spark("checkins_bronze")
    df = generate_checkins_df(spark)

    df = df.withColumn("checkin_date", to_date(df["timestamp"]))
    write_partitioned_delta(
            df, f"{BRONZE_PATH}/checkins", 
            partition_col="checkin_date",
            asset_name="checkins"
    )

    # These need to happen before spark.stop()
    yield AssetMaterialization(asset_key="checkins_bronze", metadata={"row_count": df.count()})
    yield Output(None)

    # ✅ Move this down here
    spark.stop()

    # Success marker
    success_path = f"./tmp/success/checkins/_SUCCESS"
    os.makedirs(os.path.dirname(success_path), exist_ok=True)
    try:
        with open(success_path, "w") as f:
            f.write("success")
        print(f"✅ _SUCCESS written to {success_path}")
    except PermissionError as e:
        print(f"⚠️ WARNING: Could not write _SUCCESS marker: {e}")

@asset
def members_bronze():
    spark = get_spark("bronze_members")
    df = generate_members_df(spark)
    write_partitioned_delta(df, f"{BRONZE_PATH}/members", partition_by="plan")
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
