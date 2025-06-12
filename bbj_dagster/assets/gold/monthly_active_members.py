# bbj_dagster/assets/gold/monthly_active_members.py

from dagster import asset
from pyspark.sql.functions import col, countDistinct, month
from bbj_dagster.utils.logger import get_logger, with_logger
from src.spark_session import get_spark
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH

action = 'monthly_active_users'

@asset(deps=[f"checkins"], group_name="gold")
@with_logger()
def monthly_active_members() -> None:
    logger = get_logger(f"{action}_gold")

    spark = get_spark("gold_monthly_active_members")
    logger.info(f"Spark app: {spark.sparkContext.appName}")

    df_checkins = spark.read.parquet(f"{SILVER_PATH}/checkins")
   
    result = (
        df_checkins.groupBy(month("checkin_date").alias("month"))
        .agg(countDistinct("member_id").alias("monthly_active_members"))
    )

    logger.info("Writing gold/monthly_active_members")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/monthly_active_members")
