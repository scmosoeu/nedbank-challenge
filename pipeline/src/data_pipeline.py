from src.utils.dataframe_transforms import add_ingestion_timestamp
from src.utils.file_reader import read_csv_data, read_json_data
from src.utils.file_writer import write_delta_table
from pyspark.sql import SparkSession


def bronze_layer_pipeline_csv(
    session: SparkSession, 
    input_path: str, 
    output_path: str,
    datetime_format: str
) -> None:
    """
    Executes the Bronze layer ingestion pipeline for CSV source data.

    Args:
        session: Active Spark session used to read and process data.
        input_path: File system or object storage path to the raw CSV data.
        output_path: Destination path for the Bronze Delta table.
        datetime_format: Format string used to generate ingestion timestamp.
    """

    df = read_csv_data(session, input_path)
    df = add_ingestion_timestamp(df, datetime_format)
    write_delta_table(df, output_path)


def bronze_layer_pipeline_json(
    session: SparkSession, 
    input_path: str, 
    output_path: str,
    datetime_format: str
) -> None:
    """
    Executes the Bronze layer ingestion pipeline for JSON source data.

    Args:
        session: Active Spark session used to read and process data.
        input_path: File system or object storage path to the raw JSON data.
        output_path: Destination path for the Bronze Delta table.
        datetime_format: Format string used to generate ingestion timestamp.
    """

    df = read_json_data(session, input_path)
    df = add_ingestion_timestamp(df, datetime_format)
    write_delta_table(df, output_path)
    

