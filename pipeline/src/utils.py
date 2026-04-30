import yaml

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.helper import get_datetime_now
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

    logger.info(f"Adding the ingestion_timestamp field")
    return df.withColumn("ingestion_timestamp", lit(ingestion_timestamp))