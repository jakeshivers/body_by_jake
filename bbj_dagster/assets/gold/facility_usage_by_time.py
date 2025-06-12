from dagster import asset
from pyspark.sql.functions import col, date_format, hour, count
from bbj_dagster.utils.logger import with_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("facility_usage_by_time_gold")
action = 'facility_usge_by_time'

@asset(deps=["facility_usage_silver"], group_name="gold")
@with_logger
def facility_usage_by_time_gold(context):
    usage_df = spark.read.parquet(f"{SILVER_PATH}/facility_usage_silver")

    # If usage_date is DATE type, you'll need to cast it to timestamp with a default time
    # usage_df = usage_df.withColumn("usage_ts", to_timestamp("usage_date"))

    enriched = (
        usage_df
        .withColumn("day_of_week", date_format("usage_date", "EEEE"))  # e.g., Monday
        .withColumn("hour", hour("usage_date"))  # 0–23
    )

    peak_summary = (
        enriched.groupBy("facility_type", "day_of_week", "hour")
        .agg(count("*").alias("usage_count"))
    )

    peak_summary.write.mode("overwrite").parquet(f"{GOLD_PATH}/facility_usage_by_time")
    get_success_path("gold", "facility_usage_by_time")
    context.log.info("Wrote facility_usage_by_time gold model + _SUCCESS marker.")
