import logging
from pyspark.sql import SparkSession
from config import Config

def get_spark_session(app_name: str = Config.APP_NAME) -> SparkSession:
    """
    Creates or retrieves an existing SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    # Example: adjust log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    logging.info("SparkSession created with appName '%s'.", app_name)
    return spark
