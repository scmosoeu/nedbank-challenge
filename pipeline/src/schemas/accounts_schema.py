import pyspark.sql.types as T
import pyspark.sql.functions as F 

from pyspark.sql import DataFrame


def update_accounts_schema(df: DataFrame) -> DataFrame:
    """
    Update to a standardised schema for the accounts 
    DataFrame by casting columns to their expected 
    data types.

    Args:
        df: Input Spark DataFrame containing raw account data.

    Returns:
        A Spark DataFrame with all columns cast to their
        standardised schema.
    """

    standard_df = (
        df.withColumn("open_date", F.col("open_date").cast(T.DateType()))
        .withColumn("mobile_number", F.col("mobile_number").cast(T.StringType()))
        .withColumn("credit_limit", F.col("credit_limit").cast(T.DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(T.DecimalType(18, 2)))
        .withColumn("last_activity_date", F.col("last_activity_date").cast(T.DateType()))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(T.TimestampType()))
    )

    return standard_df