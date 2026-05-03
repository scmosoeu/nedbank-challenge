from pyspark.sql import DataFrame

from src.utils.logging_utils import get_logger


logger = get_logger(__name__)

def write_delta_table(df: DataFrame, path: str) -> None:
	"""
	Write the provided DataFrame using the Delta format,
    appending to any existing data at the destination path. 
	Data is compressed using gzip to reduce storage size.

    Args:
        df: The Spark DataFrame to be written.
        path: Object storage path where the Delta
            table is stored.
    
	Returns:
        None
	"""

	logger.info(f"Ingesting data at {path}")

	df.write.format("delta") \
        .mode("append") \
        .option("compression", "uncompressed") \
        .save(path)