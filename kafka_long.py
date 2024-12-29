import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    sum as sum_,
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')

def get_spark_session(app_name: str = "HighVolumeTransactionAnalysis") -> SparkSession:
    """
    Creates or retrieves an existing SparkSession.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def get_transaction_schema() -> StructType:
    """
    Defines the schema for the transaction data.
    """
    return StructType([
        StructField("transaction_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("amount", DecimalType(10, 2), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True)
    ])

def read_raw_transactions(
    spark: SparkSession,
    bootstrap_servers: str = "kafka:9092",
    input_topic: str = "raw_transactions"
) -> DataFrame:
    """
    Reads the raw streaming transactions from Kafka.
    
    :param spark: SparkSession instance.
    :param bootstrap_servers: Kafka broker(s).
    :param input_topic: Kafka topic to subscribe to for raw transactions.
    :return: A streaming DataFrame of raw transactions.
    """
    logging.info("Reading raw transactions from Kafka topic: %s", input_topic)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .load()
    )

def parse_transactions(raw_df: DataFrame, schema: StructType, watermark: str = "5 minutes") -> DataFrame:
    """
    Parses raw Kafka data into structured columns using the given schema 
    and applies watermarking for late data.

    :param raw_df: The raw DataFrame read from Kafka.
    :param schema: The schema to parse the JSON data.
    :param watermark: Watermark duration for handling late data.
    :return: A DataFrame with parsed and timestamp-watermarked transaction data.
    """
    logging.info("Parsing raw transaction data and applying watermark of %s", watermark)
    return (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withWatermark("timestamp", watermark)
    )

def detect_high_risk_transactions(
    parsed_df: DataFrame,
    tx_count_threshold: int = 10,
    amount_threshold: float = 10000.0
) -> DataFrame:
    """
    Performs aggregation to detect high-risk transactions based on transaction 
    count and total amount within a time window.

    :param parsed_df: Parsed DataFrame with columns [transaction_id, user_id, amount, timestamp].
    :param tx_count_threshold: The threshold for transaction count in a given window.
    :param amount_threshold: The threshold for total transaction amount in a given window.
    :return: A DataFrame of high-risk transactions for the specified window and user.
    """
    logging.info(
        "Detecting high-risk transactions (tx_count > %d or total_amount > %f)",
        tx_count_threshold,
        amount_threshold
    )
    return (
        parsed_df
        .groupBy(window(col("timestamp"), "1 minute"), col("user_id"))
        .agg(
            count("*").alias("tx_count"),
            sum_("amount").alias("total_amount")
        )
        .where((col("tx_count") > tx_count_threshold) | (col("total_amount") > amount_threshold))
    )

def write_high_risk_transactions(
    risk_df: DataFrame,
    bootstrap_servers: str = "kafka:9092",
    output_topic: str = "risk_alerts",
    checkpoint_path: str = "/checkpoints/risk_alerts"
):
    """
    Writes the high-risk transactions to a Kafka topic.

    :param risk_df: DataFrame containing the identified high-risk transactions.
    :param bootstrap_servers: Kafka broker(s).
    :param output_topic: Kafka topic for publishing high-risk transactions.
    :param checkpoint_path: Filesystem path for checkpoint data.
    :return: StreamingQuery object representing the streaming write process.
    """
    logging.info("Writing high-risk transactions to Kafka topic: %s", output_topic)
    return (
        risk_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

def process_high_volume_transactions(
    bootstrap_servers: str = "kafka:9092",
    input_topic: str = "raw_transactions",
    output_topic: str = "risk_alerts",
    checkpoint_path: str = "/checkpoints/risk_alerts"
):
    """
    Main entry point that orchestrates the entire flow:
      1. Read from Kafka
      2. Parse transactions
      3. Detect high-risk transactions
      4. Publish results to a Kafka 'risk alerts' topic

    :param bootstrap_servers: Kafka broker(s).
    :param input_topic: Kafka topic with raw transactions.
    :param output_topic: Kafka topic where risk alerts are published.
    :param checkpoint_path: Filesystem path for checkpoint data.
    :return: A StreamingQuery object for the running write stream.
    """
    spark = get_spark_session()
    schema = get_transaction_schema()

    try:
        # Read raw Kafka data
        raw_df = read_raw_transactions(
            spark, 
            bootstrap_servers=bootstrap_servers,
            input_topic=input_topic
        )

        # Parse and transform data
        parsed_df = parse_transactions(raw_df, schema)

        # Detect high-risk transactions
        risk_df = detect_high_risk_transactions(parsed_df)

        # Write high-risk transactions to Kafka
        query = write_high_risk_transactions(
            risk_df,
            bootstrap_servers=bootstrap_servers,
            output_topic=output_topic,
            checkpoint_path=checkpoint_path
        )

        logging.info("Streaming query started successfully.")
        return query

    except Exception as e:
        logging.error("Failed to process high-volume transactions: %s", e, exc_info=True)
        # In real-world usage, you might want to raise this or handle it further.
        raise
