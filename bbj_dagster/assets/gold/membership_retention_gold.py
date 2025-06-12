from dagster import asset
from pyspark.sql.functions import col, min as _min, max as _max, count, datediff, current_date, when, round
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("member_retention_gold")
action = 'member_retention'

@with_logger()
@asset(group_name="gold")
def membership_retention_gold(context):
    members_df = spark.read.parquet(f"{SILVER_PATH}/members_silver")
    checkins_df = spark.read.parquet(f"{SILVER_PATH}/checkins_silver")

    checkin_stats = (
        checkins_df.groupBy("member_id")
        .agg(
            _min("checkin_date").alias("first_checkin_date"),
            _max("checkin_date").alias("last_checkin_date"),
            count("*").alias("total_checkins"),
        )
    )

    df = (
        members_df.join(checkin_stats, on="member_id", how="left")
        .withColumn("days_since_last_checkin", datediff(current_date(), col("last_checkin_date")))
        .withColumn("days_since_first_checkin", datediff(current_date(), col("first_checkin_date")))
        .withColumn("weeks_active", when(col("days_since_first_checkin") < 7, 1)
                                    .otherwise((col("days_since_first_checkin") / 7)))
        .withColumn("avg_checkins_per_week", round(col("total_checkins") / col("weeks_active"), 2))
        .withColumn("active_in_last_30_days", when(col("days_since_last_checkin") <= 30, 1).otherwise(0))
        .withColumn("is_cancelled", when(col("cancel_date").isNotNull(), 1).otherwise(0))
    )

    df.write.mode("overwrite").parquet(f"{GOLD_PATH}/member_retention")
    get_success_path("gold", "member_retention")
    context.log.info("Wrote member_retention gold model + _SUCCESS marker.")
