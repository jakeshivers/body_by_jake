from dagster import asset
from pyspark.sql.functions import col, to_date, sum as _sum
from bbj_dagster.utils.logger import with_logger
from bbj_dagster.config.constants import SILVER_PATH, GOLD_PATH, get_success_path
from src.spark_session import get_spark

spark = get_spark("retail_gold")

@asset(group_name="gold")
def retail_revenue_gold(context):
    df = spark.read.parquet(f"{SILVER_PATH}/retail_silver")
    df = df.withColumn("purchase_date", to_date("timestamp"))

    # DAILY REVENUE
    daily_revenue = (
        df.groupBy("purchase_date")
        .agg(_sum("price").alias("daily_revenue"))
    )
    daily_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_daily")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/retail_revenue_daily")
    context.log.info("Retail revenue gold aggregates completed successfully.")



    # REVENUE BY PRODUCT
    product_revenue = (
        df.groupBy("product_name")
        .agg(_sum("price").alias("product_revenue"))
    )
    product_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_by_product")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/retail_revenue_by_product")
    context.log.info("Retail revenue gold aggregates completed successfully.")
    
    # REVENUE BY MEMBER
    member_revenue = (
        df.groupBy("member_id")
        .agg(_sum("price").alias("member_revenue"))
    )
    member_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_by_member")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/monthly_active_members")
    context.log.info("Wrote MAM_behavior gold model + _SUCCESS marker.")


    # OPTIONAL: REVENUE BY DATE + PRODUCT (good for dashboards)
    daily_product_revenue = (
        df.groupBy("purchase_date", "product_name")
        .agg(_sum("price").alias("daily_product_revenue"))
    )
    daily_product_revenue.write.mode("overwrite").parquet(f"{GOLD_PATH}/retail_revenue_by_day_product")
    result.write.mode("overwrite").parquet(F"{GOLD_PATH}/retail_revenue_by_day_product")
    context.log.info("Retail revenue gold aggregates completed successfully.")
