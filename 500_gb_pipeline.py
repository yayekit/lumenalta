from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import datetime

def create_optimize_spark_session():
	"""creating an optimized spark session for large data processing"""
	return SparkSession.builder \