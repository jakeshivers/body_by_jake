from pyspark.sql.functions import current_date, date_sub, rand, floor, col, expr

class_list = ["Yoga", "Spin", "Stretch", "Peleton", "Aerobics"]
class_expr_values = ",".join([f"'{c}'" for c in class_list])

def generate_checkins_df(spark):
    N = 20_000_000

    df = spark.range(1, N + 1).toDF("checkin_id") \
        .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
        .withColumn("class_name", expr(f"element_at(array({class_expr_values}), int(rand() * {len(class_list)}) + 1)")) \
        .withColumn("duration_mins", (rand() * 60 + 20).cast("int")) \
        .withColumn("days_offset", floor(rand() * 365).cast("int")) \
        .withColumn("timestamp", date_sub(current_date(), col("days_offset")))

    return df
