from dagster import asset
from pyspark.sql.functions import col, to_date, sum as _sum
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("retail_gold")

@asset(group_name="gold", deps=["retail_silver"])
def retail_revenue_gold(context):
    df = spark.read.parquet(f"{SILVER_PATH}/retail")
    df = df.withColumn("purchase_date", to_date("timestamp"))

    # DAILY REVENUE
    daily_revenue = (
        df.groupBy("purchase_date")
        .agg(_sum("price").alias("daily_revenue"))
    )
    daily_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_daily")
    get_success_path("gold", "retail_revenue_daily")

    context.log.info("Retail daily revenue gold aggregates completed successfully.")

    # REVENUE BY PRODUCT
    product_revenue = (
        df.groupBy("product_name")
        .agg(_sum("price").alias("product_revenue"))
    )

    product_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_by_product")
    get_success_path("gold", "retail_revenue_by_product")
    logger.info("Retail revenue gold aggregates completed successfully.")

    # REVENUE BY MEMBER
    member_revenue = (
        df.groupBy("member_id")
        .agg(_sum("price").alias("member_revenue"))
    )

    member_revenue.write.mode("overwrite").parquet(F"{GOLD_PATH}/member_revenue")
    get_success_path("gold", "retail_revenue_by_demo")
    logger.info("Wrote member_revenue gold model + _SUCCESS marker.")

    # OPTIONAL: REVENUE BY DATE + PRODUCT (good for dashboards)
    daily_product_revenue = (
        df.groupBy("purchase_date", "product_name")
        .agg(_sum("price").alias("daily_product_revenue"))
    )
    daily_product_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/daily_product_revenue")
    get_success_path("gold", "daily_product_revenue")
    logger.info("Wrote daily_product_revenue gold model + _SUCCESS marker.")
