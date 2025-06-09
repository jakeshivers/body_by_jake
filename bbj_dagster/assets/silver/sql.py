from bbj_dagster.utils.activity_date_validation import enforce_member_validity
from dagster import asset
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from bbj_dagster.utils.logger import with_logger, get_logger
from bbj_dagster.config.constants import BRONZE_PATH, SILVER_PATH, get_success_path
from src.spark_session import get_spark
import pandas

spark = get_spark("sql_example")

df = spark.read.parquet(f"{SILVER_PATH}/checkins")

df.createOrReplaceTempView("checkins")

df_sql = spark.sql("""
select *
from checkins
order by checkin_date desc

""")


df_sql.show(50)