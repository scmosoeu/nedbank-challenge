import yaml

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.logger import get_logger


logger = get_logger(__name__)

def read_yaml(path: str) -> dict:
    """
    Read a YAML file and return the contents as a dictionary.
    
    Args:
        path: The path to the YAML file.

    Returns:
        A dictionary containing the contents of the YAML file.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file is not valid YAML.  
    """

    logger.info(f"Reading YAML file: {path}")
    try:
        with open(path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file: {path}") from e
    except Exception as e:
        raise Exception(f"Error reading YAML file: {path}") from e


def read_csv_data(session: SparkSession, path: str) -> DataFrame:
    """
    Read a csv file and return the contents as a dataframe

    Args:
        session: A pyspark session
        path: The path to the csv file

    Returns:
        A spark dataframe containing the contents of
        the csv file
    """

    logger.info(f'Reading csv file: {path}')
    spark_df = session.read.csv(path=path, header=True)

    return spark_df


def get_datetime_now(datetime_format: str) -> str:
    """
    Get the current date when the function is called

    Args:
        datetime_format: The format of how the datetime
            field will be recorded
    Returns:
        The date when the pipeline ran
    """
    
    datetime_now = datetime.now()

    return datetime.strftime(datetime_now, datetime_format)


def add_ingestion_timestamp(
    df: DataFrame, 
    ingestion_timestamp: str
) -> DataFrame:
    """
    Append the ingestion timestamp to the dataframe

    Args:
        df: A spark dataframe containing contents from the 
            data read
        ingestion_timestamp: The time the data was ingested
    
    Returns:
        A spark dataframe with an additional ingestion_timestamp
        field added
    """

    logger.info(f"Adding the ingestion_timestamp field")
    return df.withColumn("ingestion_timestamp", lit(ingestion_timestamp))