from src.spark_session import get_spark
from generate_data.generate_members import generate_members_df
from generate_data.generate_checkins import generate_checkins_df
from generate_data.generate_facility_usage import generate_facility_usage_df
from generate_data.generate_cancellations import generate_cancellations_df
from generate_data.generate_retail import generate_retail_df
from bbj_dagster.utils.bronze_utils import bronze_asset_op
from bbj_dagster.config.constants import DAILY_PARTITIONS
from dagster import (
                    asset, 
                    AssetMaterialization, 
                    AssetExecutionContext,
                    Output
                )

@asset(partitions_def=DAILY_PARTITIONS)
def checkins_bronze(context: AssetExecutionContext):
    spark = get_spark("checkins_bronze")
    df = generate_checkins_df(spark)
        
    df = bronze_asset_op(
            spark=spark, 
            df=df, 
            asset_name="checkins", 
            partition_col="timestamp",
    )

    yield AssetMaterialization(asset_key="checkins_bronze", metadata={"row_count": df.count()})
    yield Output(None)
    spark.stop()

@asset(partitions_def=DAILY_PARTITIONS)
def members_bronze(context: AssetExecutionContext):
    spark = get_spark("members_bronze")
    df = generate_members_df(spark)
    
    df = bronze_asset_op(
            spark=spark, 
            df=df, 
            asset_name="members", 
            partition_col="timestamp",
    )
    yield AssetMaterialization(asset_key="members_bronze", metadata={"row_count": df.count()})
    yield Output(None)
    spark.stop()

@asset(partitions_def=DAILY_PARTITIONS)
def facility_usage_bronze(context: AssetExecutionContext):
    spark = get_spark("facility_usage_bronze")
    df = generate_facility_usage_df(spark)
   
    df = bronze_asset_op(
            spark=spark, 
            df=df, 
            asset_name="facility_usage", 
            partition_col="timestamp",
    )

    yield AssetMaterialization(asset_key="facility_usage_bronze", metadata={"row_count": df.count()})
    yield Output(None)
    spark.stop()

@asset(partitions_def=DAILY_PARTITIONS)
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

@asset(partitions_def=DAILY_PARTITIONS)
def retail_bronze(context: AssetExecutionContext):
    spark = get_spark("retail_bronze")

    df = generate_retail_df(spark)
    
    df = bronze_asset_op(
            spark=spark, 
            df=df, 
            asset_name="retail", 
            partition_col="timestamp",
    )

    yield AssetMaterialization(asset_key="retail_bronze", metadata={"row_count": df.count()})
    yield Output(None)
    spark.stop()