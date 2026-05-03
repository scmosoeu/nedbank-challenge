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


def add_age_bands(df: DataFrame) -> DataFrame:
    """
    Enriches the input DataFrame with age-derived features based on date of birth
    and pipeline ingestion timestamp.

    Args:
        df: Input Spark DataFrame containing at minimum:
            - ingestion_timestamp (timestamp or string castable to date)
            - dob (date of birth)
    
    Returns:
        A Spark DataFrame enriched with age_band as categorical
        features
    """
    

    df= (
        df.withColumn("pipeline_run_date", F.col('ingestion_timestamp').cast(T.DateType()))
        .withColumn("age", F.floor(F.datediff(F.col("pipeline_run_date"), F.col("dob")) / 365.25))
        .withColumn(
            "age_band",
            F.when(F.col("age") >= 65, "65+")
            .when(F.col("age") >= 56, "56-65")
            .when(F.col("age") >= 46, "46-55")
            .when(F.col("age") >= 36, "36-45")
            .when(F.col("age") >= 26, "26-35")
            .when(F.col("age") >= 18, "18-25")
            .otherwise(None)
        )
    )

    return df



def update_customers_schema_gold(df: DataFrame) -> DataFrame:
    """
    Update the accounts schema to gold layer schema

    Args:
        df: Input Spark DataFrame containing silver layer
            schema.

    Returns:
        A Spark DataFrame containing gold standard schema.
    """

    df = add_age_bands(df)

    gold_df = df.withColumn(
        "customers_sk", F.xxhash64("customer_id") # cast to bigint
    ).drop("id_number", "first_name", "last_name", "dob")
    
    return gold_df