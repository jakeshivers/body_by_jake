import os
from pyspark.sql import DataFrame

def write_partitioned_delta(df: DataFrame, path: str, partition_col: str, asset_name: str):
    print(f"💾 Writing partitioned Delta to {path} on {partition_col}...")

    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .option("mergeSchema", "true") \
        .save(path)

    print("✅ Delta write complete. Proceeding to _SUCCESS marker...")

    success_path = f"./tmp/success/{asset_name}/_SUCCESS"
    os.makedirs(os.path.dirname(success_path), exist_ok=True)

    try:
        with open(success_path, "w") as f:
            f.write("success")
        print(f"✅ _SUCCESS written to {success_path}")
    except Exception as e:
        print(f"⚠️ WARNING: Could not write _SUCCESS marker: {e}")
