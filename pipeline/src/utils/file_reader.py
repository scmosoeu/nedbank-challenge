from pyspark.sql import DataFrame, SparkSession

from src.utils.logging_utils import get_logger


logger = get_logger(__name__)
        
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
    spark_df = (
        session.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(path)
    )

    return spark_df


def read_json_data(session: SparkSession, path: str) -> DataFrame:
    """
    Read a json file and return the contents 
    as a dataframe

    Args:
        session: A pyspark session
        path: The path to the json file

    Returns:
        A spark dataframe containing the contents of
        the json file
    """

    logger.info(f'Reading json file: {path}')
    spark_df = (
        session.read.format("json")
                    .load(path)
    )

    return spark_df


def read_delta_table(session: SparkSession, path: str) -> DataFrame:
    """
    Read a Delta table from the specified path into a Spark DataFrame.

    Args:
        session: The active Spark session used to perform the 
            read operation.
        path: The object storage path where the Delta table 
            is stored.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the 
            data read from the Delta table.
    """
    
    spark_df = session.read.format("delta").load(path)

    return spark_df
