import pyspark.sql.types as T
import pyspark.sql.functions as F 

from pyspark.sql import DataFrame


def update_accounts_schema_silver(df: DataFrame) -> DataFrame:
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


def update_accounts_schema_gold(df: DataFrame) -> DataFrame:
    """
    Update the accounts schema to gold layer schema

    Args:
        df: Input Spark DataFrame containing silver layer
            schema.

    Returns:
        A Spark DataFrame containing gold standard schema.
    """

    df = df.withColumnRenamed('customer_ref', 'customer_id')

    gold_df = df.withColumn(
        "accounts_sk", F.xxhash64("account_id") # cast to bigint
    ).drop(
        "mobile_number", "ingestion_timestamp"
    )
    
    return gold_df