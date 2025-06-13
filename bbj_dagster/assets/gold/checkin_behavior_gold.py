from dagster import asset
from pyspark.sql.functions import (
    col, min as _min, max as _max, countDistinct, count,
    when, dayofweek, datediff, current_date, round
)
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark
from dagster import (
                    asset, 
                    AssetMaterialization, 
                    AssetExecutionContext,
                    Output
                )

@asset( group_name="gold")
def checkin_behavior_gold(context):
    spark = get_spark("checkin_behavior_gold")
    
    logger = get_logger("checkin_behavior_gold")
    df = spark.read.parquet(f"{SILVER_PATH}/checkins_silver")
    # 1 = Sunday, 7 = Saturday in Spark dayofweek
    enriched = (
        df.withColumn("day_of_week", dayofweek("checkin_date"))
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
    )

    agg_df = (
        enriched.groupBy("member_id")
        .agg(
            _min("checkin_date").alias("first_checkin_date"),
            _max("checkin_date").alias("last_checkin_date"),
            count("*").alias("total_checkins"),
            countDistinct("checkin_date").alias("active_days"),
            count(when(~col("is_weekend").cast("boolean"), True)).alias("weekday_checkins"),
            count(when(col("is_weekend") == 1, True)).alias("weekend_checkins"),
            datediff(current_date(), _min("checkin_date")).alias("days_active")
        )
        .withColumn(
            "avg_checkins_per_week",
            round(col("total_checkins") / (col("days_active") / 7), 2)
        )
    )

    agg_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/checkin_behavior")
    logger.info(f"Wrote silver data to: {SILVER_PATH}/{action}")
    
    get_success_path("gold", "checkin_behavior")
    logger.info("Wrote _SUCCESS marker")
