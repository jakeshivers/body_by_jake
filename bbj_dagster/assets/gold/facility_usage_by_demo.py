from dagster import asset
from pyspark.sql.functions import col, year, current_date, when, count
from bbj_dagster.utils.logger import with_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("facility_usage_by_demo_gold")
action = 'facility_usage'

@asset(deps=["facility_usage_silver", "members_silver"], group_name="gold")
@with_logger
def facility_usage_by_demo_gold(context):
    usage_df = spark.read.parquet(f"{SILVER_PATH}/facility_usage_silver")
    members_df = spark.read.parquet(f"{SILVER_PATH}/members_silver")

    enriched = (
        usage_df.join(members_df, on="member_id", how="left")
        .withColumn("member_age", year(current_date()) - col("birth_year"))
        .withColumn("age_group", when(col("member_age") < 25, "<25")
                    .when(col("member_age").between(25, 34), "25–34")
                    .when(col("member_age").between(35, 49), "35–49")
                    .otherwise("50+"))
    )

    demo_summary = (
        enriched.groupBy("facility_type", "gender", "age_group")
        .agg(count("*").alias("usage_count"))
    )

    demo_summary.write.mode("overwrite").parquet(f"{GOLD_PATH}/facility_usage_by_demo")
    get_success_path("gold", "facility_usage_by_demo")
    context.log.info("Wrote facility_usage_by_demo gold model + _SUCCESS marker.")
