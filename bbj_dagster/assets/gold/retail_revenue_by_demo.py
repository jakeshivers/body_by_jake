from dagster import asset
from pyspark.sql.functions import col, year, current_date, when, sum as _sum
from bbj_dagster.utils.logger import with_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("retail_revenue_by_demo_gold")
action = 'retail_revenue_by_demo'

@asset(deps=[f"retail_silver", "members_silver"], group_name="gold")
@with_logger
def retail_revenue_by_demo_gold(context):
    retail_df = spark.read.parquet(f"{SILVER_PATH}/retail_silver")
    members_df = spark.read.parquet(f"{SILVER_PATH}/members_silver")

    enriched = (
        retail_df.join(members_df, on="member_id", how="left")
        .withColumn("member_age", year(current_date()) - col("birth_year"))
        .withColumn("age_group", when(col("member_age") < 25, "<25")
                    .when(col("member_age").between(25, 34), "25–34")
                    .when(col("member_age").between(35, 49), "35–49")
                    .otherwise("50+"))
    )

    demo_summary = (
        enriched.groupBy("product_name", "gender", "age_group")
        .agg(_sum("price").alias("total_revenue"))
    )

    demo_summary.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_by_demo")
    get_success_path("gold", "retail_revenue_by_demo")
    context.log.info("Wrote retail_revenue_by_demo gold model + _SUCCESS marker.")
