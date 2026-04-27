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
    
    spark = SparkSession.builder.appName(
        config["spark"]["app_name"]
    ).master(
        config['spark']['master']
    )
    
    return spark.getOrCreate()