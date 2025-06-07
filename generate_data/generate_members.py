# generate_data/generate_members.py

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, floor, current_date, date_sub, col, udf
from pyspark.sql.types import StringType

fake = Faker()
plans = ["Basic", "Premium", "Elite", "Student", "Family"]
plan_expr = ",".join([f"'{p}'" for p in plans])

@udf(StringType())
def fake_name():
    return fake.name()

def generate_members_df(spark: SparkSession, n_rows: int = 1_000_000):
    df = spark.range(1, n_rows + 1).toDF("member_id") \
        .withColumn("age", (rand() * 40 + 18).cast("int")) \
        .withColumn("signup_offset", (floor(rand() * 365)).cast("int")) \
        .withColumn("timestamp", date_sub(current_date(), col("signup_offset"))) \
        .withColumn("zipcode", expr("CAST(FLOOR(rand() * 90000 + 10000) AS STRING)")) \
        .withColumn("plan", expr(f"element_at(array({plan_expr}), int(rand() * {len(plans)}) + 1)")) \
        .withColumn("name", fake_name())

    return df
