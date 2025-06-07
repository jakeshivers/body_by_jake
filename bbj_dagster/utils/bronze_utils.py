from bbj_dagster.config.constants import BRONZE_PATH, get_success_path
from pyspark.sql.functions import to_date
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, lit

def bronze_asset_op(spark, df, asset_name: str, partition_col: str):
    from pyspark.sql.functions import to_date
    import os

    df = df.withColumn(partition_col, to_date(df[partition_col]))
    target_path = f"{BRONZE_PATH}/{asset_name}"

    print(f"💾 Writing partitioned Delta to {target_path} on {partition_col}...")
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy(partition_col)
        .option("mergeSchema", "true")
        .save(target_path)
    )
    print("✅ Delta write complete.")

    try:
        partition_value = df.select(partition_col).first()[0].isoformat()
        success_path = get_success_path(asset_name, partition_value)
        os.makedirs(os.path.dirname(success_path), exist_ok=True)
        with open(success_path, "w") as f:
            f.write("success")
        print(f"✅ _SUCCESS marker written to {success_path}")
    except Exception as e:
        print(f"⚠️ Failed to write _SUCCESS marker: {e}")
    return df

def get_new_success_partitions(local_base_path, seen_set):
    new_partitions = []
    for subdir in os.listdir(local_base_path):
        path = os.path.join(local_base_path, subdir, "_SUCCESS")
        if os.path.isfile(path) and subdir not in seen_set:
            new_partitions.append(subdir)
    return new_partitions

def filter_by_partition(df: DataFrame, colname: str, partition_value: str) -> DataFrame:
    """Ensure date column exists and restrict to partition_value (e.g., '2025-06-01')."""
    df = df.withColumn(colname, to_date(df[colname]))
    return df.filter(df[colname] == lit(partition_value))