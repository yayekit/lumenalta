import logging
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import from_json, col, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Constants
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
RAW_TRANSACTIONS_TOPIC = "raw_transactions"
RISK_ALERTS_TOPIC = "risk_alerts"
CHECKPOINT_LOCATION = "/checkpoints/risk_alerts"
WATERMARK_DURATION = "5 minutes"
WINDOW_DURATION = "1 minute"
TX_COUNT_THRESHOLD = 10
TOTAL_AMOUNT_THRESHOLD = 10000

# Define schema for transaction data
TRANSACTION_SCHEMA: StructType = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("amount", DecimalType(10, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False)
])

def create_spark_session(app_name: str = "HighVolumeTransactionProcessor") -> SparkSession:
    """Initializes and returns a Spark session."""
    logger.info("Initializing Spark session.")
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_raw_stream(spark: SparkSession, schema: StructType) -> DataFrame:
    """Reads the raw transaction stream from Kafka."""
    logger.info(f"Reading raw stream from Kafka topic: {RAW_TRANSACTIONS_TOPIC}")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", RAW_TRANSACTIONS_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", WATERMARK_DURATION)
    
    logger.info("Successfully parsed raw stream.")
    return parsed_stream

def perform_risk_analysis(parsed_stream: DataFrame) -> DataFrame:
    """Performs risk analysis by aggregating transactions."""
    logger.info("Performing risk analysis on parsed stream.")
    risk_analysis = parsed_stream \
        .groupBy(
            window("timestamp", WINDOW_DURATION),
            "user_id"
        ) \
        .agg(
            count("*").alias("tx_count"),
            sum("amount").alias("total_amount")
        ) \
        .filter((col("tx_count") > TX_COUNT_THRESHOLD) | (col("total_amount") > TOTAL_AMOUNT_THRESHOLD))
    
    logger.info("Risk analysis aggregation complete.")
    return risk_analysis

def write_risk_alerts(risk_analysis: DataFrame) -> StreamingQuery:
    """Writes high-risk transactions back to Kafka."""
    logger.info(f"Writing risk alerts to Kafka topic: {RISK_ALERTS_TOPIC}")
    query = risk_analysis.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", RISK_ALERTS_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("update") \
        .start()
    
    logger.info("Risk alerts streaming query started.")
    return query

def process_high_volume_transactions() -> None:
    """Main function to process high-volume transactions."""
    try:
        spark = create_spark_session()
        parsed_stream = read_raw_stream(spark, TRANSACTION_SCHEMA)
        risk_analysis = perform_risk_analysis(parsed_stream)
        query = write_risk_alerts(risk_analysis)
        
        logger.info("Starting the streaming query. Awaiting termination...")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}", exc_info=True)
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    process_high_volume_transactions()
