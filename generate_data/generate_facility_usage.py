from pyspark.sql.functions import current_date, date_sub, rand, floor, col, expr

def generate_facility_usage_df(spark):
    N = 12_000_000
    facilities = ["Sauna", "Pool", "Steam Room", "Ice Bath"]
    facility_expr = ",".join([f"'{f}'" for f in facilities])

    df = spark.range(1, N + 1).toDF("usage_id") \
        .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
        .withColumn("duration_mins", (rand() * 80 + 10).cast("int")) \
        .withColumn("days_offset", (floor(rand() * 365)).cast("int")) \
        .withColumn("timestamp", date_sub(current_date(), col("days_offset"))) \
        .withColumn("facility", expr(f"element_at(array({facility_expr}), int(rand() * {len(facilities)}) + 1)"))

    return df
