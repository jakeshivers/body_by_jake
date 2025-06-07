from dagster import DailyPartitionsDefinition

DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2025-06-01")
BRONZE_PATH = "s3a://bbj-lakehouse/bronze"

def get_success_path(asset_name: str, partition_value: str) -> str:
    return f"./tmp/success/{asset_name}/{partition_value}/_SUCCESS"
