# bbj_dagster/utils/data_validation.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def enforce_member_validity(df: DataFrame, df_members: DataFrame, timestamp_col: str = "timestamp") -> DataFrame:
    """
    Ensures each record in `df` corresponds to a valid member who joined on or before the record's timestamp.
    
    Args:
        df (DataFrame): The event DataFrame to filter (e.g., retail, checkins, etc.)
        df_members (DataFrame): The members_silver DataFrame with 'member_id' and 'join_date'
        timestamp_col (str): The column in `df` representing the event timestamp

    Returns:
        DataFrame: A filtered DataFrame joined with member info and excluding invalid timestamps.
    """
    return (
        df
        .filter(col("member_id").isNotNull())
        .join(df_members.select("member_id", "join_date"), on="member_id", how="inner")
        .filter(col(timestamp_col) >= col("join_date"))
    )
