import pyspark.sql.types as T
import pyspark.sql.functions as F 

from pyspark.sql import DataFrame


def flatten_transactions_nested_field(df: DataFrame) -> DataFrame:
    """
    Flatten nested fields in the transactions dataset
    and drop the parent field

    Args:
        df: A spark dataframe containing nested fields

    Returns:
        A spark daframe with the nested fields flattened 
    """

    flatten_df = (
        df.select(
            "*",
            F.col("location.city").alias("city"),
            F.col("location.coordinates").alias("coordinates"),
            F.col("location.province").alias("province"),
            F.col("metadata.device_id").alias("device_id"),
            F.col("metadata.retry_flag").alias("retry_flag"),
            F.col("metadata.session_id").alias("session_id")
        ).drop("location", "metadata") # Drop nested field parent
    )

    return flatten_df


def update_transactions_schema(df: DataFrame) -> DataFrame:
    """
    Update to a standardised schema for the transactions 
    DataFrame by casting columns to their expected 
    data types.

    Args:
        df: Input Spark DataFrame containing raw transaction data.

    Returns:
        A Spark DataFrame with all columns cast to their
        standardised schema.
    """

    flatten_df = flatten_transactions_nested_field(df)

    standard_df = (
        flatten_df
            .withColumn("transaction_date", F.col("transaction_date").cast(T.DateType()))
            .withColumn("retry_flag", F.col("retry_flag").cast(T.BooleanType()))
            .withColumn('currency', F.lit('ZAR'))
    )
   
    return standard_df
