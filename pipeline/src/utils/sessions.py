import os
import delta
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

    delta_path = os.path.dirname(delta.__file__)
    jars_path = os.path.join(delta_path, "jars", "*")
    
    spark = (
        SparkSession.builder 
        .appName(config["spark"]["app_name"]) 
        .master(config["spark"]["master"])
        .config("spark.jars", jars_path)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # By default, plain Apache Spark doesn’t include Delta support. You have to add it explicitly.

    return spark
