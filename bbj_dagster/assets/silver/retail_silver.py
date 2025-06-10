from bbj_dagster.utils.activity_date_validation import enforce_member_validity
from dagster import asset
from pyspark.sql.functions import col
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import BRONZE_PATH, SILVER_PATH, get_success_path
from src.spark_session import get_spark

action = "retail"

@asset(deps=[f"{action}_bronze", "members_silver"], group_name="silver")
@with_logger()
def retail_silver():
    logger = get_logger(f"{action}_silver")
   
    spark = get_spark(f"silver_{action}_transform")
    logger.info(f"Spark app: {spark.sparkContext.appName}")

    df_retail_bronze = spark.read.parquet(f"{BRONZE_PATH}/{action}")
    df_members = spark.read.parquet(f"{SILVER_PATH}/members")
    df_silver = enforce_member_validity(df_retail_bronze, df_members, timestamp_col="timestamp")

    logger.info(f"Read {df_retail_bronze.count()} rows from BRONZE")

    df_silver = (
        df_silver
        .filter(col("member_id") > 0 )
        .filter(col("price") > 1 ) 
        .withColumn(f"{action}_date", col("timestamp"))
    )

    key_cols = ["purchase_id", "member_id"]
    non_key_cols = [c for c in df_silver.columns if c not in key_cols]

    df_silver = df_silver.select(*key_cols, *non_key_cols)

    logger.info(f"Silver DataFrame has {df_silver.count()} rows")
    logger.info("Schema:\n" + df_silver._jdf.schema().treeString())

    df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/{action}")
    logger.info(f"Wrote silver data to: {SILVER_PATH}/{action}")

    get_success_path("silver", action)
    logger.info("Wrote _SUCCESS marker")