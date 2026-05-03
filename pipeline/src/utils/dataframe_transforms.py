import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.helper.datetime_helper import get_datetime_now
from src.helper.logger import get_logger


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

    ingestion_timestamp = get_datetime_now(datetime_format)

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