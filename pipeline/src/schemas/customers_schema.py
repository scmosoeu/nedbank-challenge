import pyspark.sql.types as T
import pyspark.sql.functions as F 

from pyspark.sql import DataFrame


def update_customers_schema_silver(df: DataFrame) -> DataFrame:
    """
    Update to a standardised schema for the customers 
    DataFrame by casting columns to their expected 
    data types.

    Args:
        df: Input Spark DataFrame containing raw customer data.

    Returns:
        A Spark DataFrame with all columns cast to their
        standardised schema.
    """

    standard_df = (
        df.withColumn("dob", F.col("dob").cast(T.DateType()))
        .withColumn("risk_score", F.col("risk_score").cast(T.IntegerType()))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(T.TimestampType()))
    )

    return standard_df