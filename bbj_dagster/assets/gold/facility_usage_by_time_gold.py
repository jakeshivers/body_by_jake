from dagster import asset
from pyspark.sql.functions import col, date_format, hour, count
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("facility_usage_by_time_gold")
action = 'facility_usage_by_time'

@asset(group_name="gold", deps=["facility_usage_silver"])
@with_logger()
def facility_usage_by_time_gold(context):
    logger = get_logger(action)

    usage_df = spark.read.parquet(f"{SILVER_PATH}/facility_usage")

    enriched = (
        usage_df
        .withColumn("day_of_week", date_format("timestamp", "EEEE"))  # Use "timestamp" directly
        .withColumn("hour", hour("timestamp"))
    )

    peak_summary = (
        enriched.groupBy("facility", "day_of_week", "hour")
        .agg(count("*").alias("usage_count"))
    )

    peak_summary.write.mode("overwrite").parquet(f"{GOLD_PATH}/{action}")
    get_success_path("gold", action)
    logger.info(f"Wrote gold data to: {GOLD_PATH}/{action}")
    logger.info("Wrote _SUCCESS marker")
