from dagster import asset
from pyspark.sql.functions import col, year, current_date, when
from bbj_dagster.utils.logger import with_logger
from bbj_dagster.config.constants import GOLD_PATH, SILVER_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("features_churn_gold")
action = 'features_churn'

@with_logger
@asset( group_name="gold")
def features_churn_gold(context):
    members_df = spark.read.parquet(f"{SILVER_PATH}/members_silver")
    retention_df = spark.read.parquet(f"{GOLD_PATH}/member_retention")
    checkin_df = spark.read.parquet(f"{GOLD_PATH}/checkin_behavior")

    base = (
        members_df
        .withColumn("member_age", year(current_date()) - col("birth_year"))
        .withColumn("age_group", when(col("member_age") < 25, "<25")
                    .when(col("member_age").between(25, 34), "25–34")
                    .when(col("member_age").between(35, 49), "35–49")
                    .otherwise("50+"))
    )

    features = (
        base.join(retention_df.select("member_id", "is_cancelled", "active_in_last_30_days"), on="member_id", how="left")
            .join(checkin_df.select("member_id", "avg_checkins_per_week", "total_checkins", "weekday_checkins", "weekend_checkins"), on="member_id", how="left")
    )

    features.write.mode("overwrite").parquet(f"{GOLD_PATH}/features_churn")
    get_success_path("gold", "features_churn")
    context.log.info("Wrote features_churn gold model + _SUCCESS marker.")
