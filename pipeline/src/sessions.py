from pyspark.sql import SparkSession


def get_spark_session(config: dict) -> SparkSession:
    """
    Get a Spark session with Delta Lake support.

    Args:
        config: A dictionary containing the configuration 
        for the Spark session.
    Returns:
        A Spark session with Delta Lake support.
    """
    
    # By default, plain Apache Spark doesn’t include Delta support. You have to add it explicitly.
    return SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .master(config["spark"]["master"]) \
        .getOrCreate()
