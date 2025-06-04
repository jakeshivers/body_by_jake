from pyspark.sql.functions import current_date, date_sub, rand, floor, col, expr

def generate_retail_df(spark):
    N = 15_000_000
    products = [
        ("Protein Bar", 2.99),
        ("Preworkout Drink", 3.99),
        ("Creatine Powder", 19.99),
        ("Electrolyte Mix", 1.99),
        ("Gym Shirt", 24.99),
        ("Gym Shorts", 29.99),
        ("Whey Protein", 39.99)
    ]

    product_expr = ",".join(
        [f"struct('{name}' AS product_name, {price} AS price)" for name, price in products]
    )

    df = spark.range(1, N + 1).toDF("purchase_id") \
        .withColumn("member_id", (rand() * 1_000_000).cast("int")) \
        .withColumn("days_offset", (floor(rand() * 365)).cast("int")) \
        .withColumn("timestamp", date_sub(current_date(), col("days_offset"))) \
        .withColumn(
            "product_struct",
            expr(f"element_at(array({product_expr}), int(rand() * {len(products)}) + 1)")
        ) \
        .select(
            "purchase_id",
            "member_id",
            "timestamp",
            col("product_struct.product_name").alias("product_name"),
            col("product_struct.price").alias("price")
        )

    return df
