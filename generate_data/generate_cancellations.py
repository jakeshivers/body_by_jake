from pyspark.sql.functions import current_date, date_sub, rand, floor, col, expr

reasons = [
    "Too expensive",
    "Not using enough",
    "Moved",
    "Found another gym",
    "No longer interested",
    "Medical reasons",
    "Poor facility experience"
]

reason_expr = ",".join([f"'{r}'" for r in reasons])

def generate_cancellations_df(spark):
    N = 75_000

    df = spark.range(1, N + 1).toDF("cancel_id") \
        .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
        .withColumn("days_offset", (floor(rand() * 365)).cast("int")) \
        .withColumn("timestamp", date_sub(current_date(), col("days_offset"))) \
        .withColumn("reason", expr(f"element_at(array({reason_expr}), int(rand() * {len(reasons)}) + 1)"))

    return df
