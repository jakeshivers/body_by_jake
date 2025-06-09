import os
from dagster import DailyPartitionsDefinition

DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2025-06-01")
BRONZE_PATH = "s3a://bbj-lakehouse/bronze"
SILVER_PATH = "s3a://bbj-lakehouse/silver"

def get_success_path(layer: str, table: str) -> str:
    success_path = f"./tmp/success/{layer}/{table}/_SUCCESS"
    os.makedirs(os.path.dirname(success_path), exist_ok=True)
    with open(success_path, "w") as f:
        f.write("")
    return success_path