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

    if asset_name:
        local_path = path.replace("s3a://bbj-lakehouse", "/mnt/data/minio")
        success_path = f"./tmp/success/{asset_name}/_SUCCESS"
        os.makedirs(os.path.dirname(success_path), exist_ok=True)
        try:
            with open(success_path, "w") as f:
                f.write("success")
            print(f"✅ _SUCCESS written to {success_path}")
        except PermissionError as e:
            print(f"⚠️ WARNING: Could not write _SUCCESS marker: {e}")
