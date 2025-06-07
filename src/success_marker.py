# src/success_marker.py
from pyspark.sql import SparkSession, Row
from datetime import datetime

def write_success_marker(spark: SparkSession, layer: str, asset_name: str):
    marker_path = f"s3a://bbj-lakehouse/_markers/{layer}/{asset_name}/_SUCCESS"
    marker_df = spark.createDataFrame([Row(success=True, ts=datetime.utcnow().isoformat())])
    marker_df.write.mode("overwrite").json(marker_path)
    print(f"✅ _SUCCESS marker written to {marker_path}")
