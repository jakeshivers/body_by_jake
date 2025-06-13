from src.spark_session import get_spark
from generate_data.generate_cancellations import generate_cancellations_df

from bbj_dagster.utils.bronze_utils import bronze_asset_op
from bbj_dagster.utils.logger import with_logger, get_logger

from bbj_dagster.config.constants import DAILY_PARTITIONS
from dagster import (
                    asset, 
                    AssetMaterialization, 
                    AssetExecutionContext,
                    Output
                )
@asset(partitions_def=DAILY_PARTITIONS, group_name="bronze")
@with_logger()
def cancellations_bronze(context: AssetExecutionContext):
    spark = get_spark("cancellations_bronze")
    df = generate_cancellations_df(spark)
    
    df = bronze_asset_op(
            spark=spark, 
            df=df, 
            asset_name="cancellations", 
            partition_col="timestamp",
    )

    yield AssetMaterialization(asset_key="cancellations_bronze", metadata={"row_count": df.count()})
    yield Output(None)
    spark.stop()