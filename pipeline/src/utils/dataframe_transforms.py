from datetime import datetime

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

from src.utils.logging_utils import get_logger


logger = get_logger(__name__)

def add_ingestion_timestamp(
    df: DataFrame, 
    datetime_format: str
) -> DataFrame:
    """
    Append the ingestion timestamp to the dataframe

    Args:
        df: A spark dataframe containing contents from the 
            data read
        datetime_format: The format the datetime variable is following,
            e.g. iso 8601 format
    
    Returns:
        A spark dataframe with an additional ingestion_timestamp
        field added
    """

    datetime_now = datetime.now()

    ingestion_timestamp = datetime.strftime(datetime_now, datetime_format)

    logger.info(f"Ingestion timestamp: {ingestion_timestamp}")
    return df.withColumn("ingestion_timestamp", F.lit(ingestion_timestamp))


def remove_duplicates(df: DataFrame, subset: list[str]) -> DataFrame:
    """
    Remove duplicate rows in a spark dataframe

    Args:
        df: A spark dataframe containing data to
            be checked for presence of duplicate
            entries
        subset: A list of columns to use for comparing
            the duplicates

    Returns:
        A spark dataframe without duplicates
    """

    logger.info("Deduplicate the dataset")
    return df.dropDuplicates(subset=subset)
