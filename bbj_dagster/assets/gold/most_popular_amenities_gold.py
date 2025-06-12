# bbj_dagster/assets/gold/monthly_active_members.py

from dagster import asset
from pyspark.sql.functions import col, countDistinct, month
from bbj_dagster.utils.logger import get_logger, with_logger
from src.spark_session import get_spark
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH

action = 'most_popular_amenities_over_time'

@with_logger()
@asset( group_name="gold")
def most_popular_amenities_gold() -> None:
    logger = get_logger(f"{action}_gold")

    spark = get_spark("gold_most_popular_amenities_over_time")
    logger.info(f"Spark app: {spark.sparkContext.appName}")

    df_checkins = spark.read.parquet(f"{SILVER_PATH}/checkins")
   
    result = (
        df_checkins.groupBy(month("checkin_date").alias("month"), "class_list")
     
    )

    logger.info("Writing gold/most_popular_amenities")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/most_popular_amenities_over_time")
