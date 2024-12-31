import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    sum as sum_
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.streaming import StreamingQuery

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')

# -----------------------------------------------------------------------------
# Spark Session Setup
# -----------------------------------------------------------------------------
def get_spark_session(app_name: str = "HighVolumeTransactionAnalysis") -> SparkSession:
    """
    Creates or retrieves an existing SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# -----------------------------------------------------------------------------
# Schema Definition
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Reading from Kafka
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Parsing Transactions
# -----------------------------------------------------------------------------
def parse_transactions(
    raw_df: DataFrame,
    schema: StructType,
    watermark: str = "5 minutes"
) -> DataFrame:
    """
    Parses raw Kafka data into structured columns using the given schema 
    and applies watermarking for late data.

    :param raw_df: The raw DataFrame read from Kafka.
    :param schema: The schema to parse the JSON data.
    :param watermark: Watermark duration for handling late data.
    :return: A DataFrame with parsed and timestamp-watermarked transaction data.
    """
    logging.info("Parsing raw transaction data with watermark of %s", watermark)
    parsed_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withWatermark("timestamp", watermark)
    )
    return parsed_df

# -----------------------------------------------------------------------------
# Validation Layer
# -----------------------------------------------------------------------------
def validate_transactions(parsed_df: DataFrame) -> DataFrame:
    """
    Filters out invalid transactions, such as negative amounts or missing critical fields
    (transaction_id, user_id). You can add more complex validation logic here.

    :param parsed_df: The parsed DataFrame.
    :return: A validated DataFrame.
    """
    logging.info("Validating parsed transaction data.")
    # Example of simple validations:
    # 1. Filter out negative amounts
    # 2. Ensure critical fields (transaction_id, user_id) are not null.
    validated_df = (
        parsed_df
        .filter(col("amount") >= 0)
        .filter(col("transaction_id").isNotNull() & col("user_id").isNotNull())
    )
    return validated_df

# -----------------------------------------------------------------------------
# Detecting High-Risk Transactions
# -----------------------------------------------------------------------------
def detect_high_risk_transactions(
    parsed_df: DataFrame,
    tx_count_threshold: int = 10,
    amount_threshold: float = 10000.0,
    window_duration: str = "1 minute"
) -> DataFrame:
    """
    Performs aggregation to detect high-risk transactions based on transaction 
    count and total amount within a specified time window.

    :param parsed_df: Parsed DataFrame with columns [transaction_id, user_id, amount, timestamp].
    :param tx_count_threshold: The threshold for transaction count in a given window.
    :param amount_threshold: The threshold for total transaction amount in a given window.
    :param window_duration: Duration of the rolling window for aggregations.
    :return: A DataFrame of high-risk transactions for the specified window and user.
    """
    logging.info(
        "Detecting high-risk transactions in %s window (tx_count > %d or total_amount > %f)",
        window_duration, tx_count_threshold, amount_threshold
    )
    risk_df = (
        parsed_df
        .groupBy(window(col("timestamp"), window_duration), col("user_id"))
        .agg(
            count("*").alias("tx_count"),
            sum_("amount").alias("total_amount")
        )
        .where(
            (col("tx_count") > tx_count_threshold) |
            (col("total_amount") > amount_threshold)
        )
    )
    return risk_df

# -----------------------------------------------------------------------------
# Writing to Kafka
# -----------------------------------------------------------------------------
def write_high_risk_transactions(
    risk_df: DataFrame,
    bootstrap_servers: str = "kafka:9092",
    output_topic: str = "risk_alerts",
    checkpoint_path: str = "/checkpoints/risk_alerts"
) -> StreamingQuery:
    """
    Writes the high-risk transactions to a Kafka topic.

    :param risk_df: DataFrame containing the identified high-risk transactions.
    :param bootstrap_servers: Kafka broker(s).
    :param output_topic: Kafka topic for publishing high-risk transactions.
    :param checkpoint_path: Filesystem path for checkpoint data.
    :return: StreamingQuery object representing the streaming write process.
    """
    logging.info("Writing high-risk transactions to Kafka topic: %s", output_topic)
    query = (
        risk_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
    return query

# -----------------------------------------------------------------------------
# Persisting to Data Lake
# -----------------------------------------------------------------------------
def persist_transactions_to_data_lake(
    parsed_df: DataFrame,
    output_path: str = "transactions_data_lake",
    checkpoint_path: str = "/checkpoints/data_lake",
    output_mode: str = "append",
    trigger_interval: str = "1 minute"
) -> StreamingQuery:
    """
    Persists all valid transactions to a data lake for archival or batch analysis.
    Here, it writes to a path (local, HDFS, or S3), typically as Parquet or Delta.

    :param parsed_df: Validated DataFrame of transactions to be stored.
    :param output_path: The target path for storing transactions (e.g., S3 URI, HDFS path).
    :param checkpoint_path: Filesystem path for checkpoint data.
    :param output_mode: Output mode for structured streaming writes (e.g., "append", "complete").
    :param trigger_interval: Time interval for micro-batch triggers.
    :return: A StreamingQuery object for the running write stream.
    """
    logging.info("Persisting transactions to data lake at path: %s", output_path)
    query = (
        parsed_df
        .writeStream
        .outputMode(output_mode)
        .format("parquet")  # or "delta"
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query

# -----------------------------------------------------------------------------
# Alerting Mechanism
# -----------------------------------------------------------------------------
def send_alerts_if_needed(risk_df: DataFrame) -> None:
    """
    A stub for sending alerts to external systems (e.g., Slack, email, or an HTTP endpoint).
    In a real scenario, you might collect the data from `risk_df` in a foreachBatch,
    or create a separate write stream that calls an alerting endpoint.

    :param risk_df: DataFrame containing high-risk transactions.
    :return: None
    """
    logging.info("Preparing to send alerts for high-risk transactions.")

    # Example approach: 
    # 1. Use `foreachBatch` to collect your risk_df to driver
    # 2. For each row, call a function or external service

    def alert_batch_function(batch_df, batch_id):
        records = batch_df.collect()
        for row in records:
            # Construct your alert message
            alert_msg = (
                f"ALERT: High-risk transaction(s) detected. "
                f"User: {row['user_id']} | TX Count: {row['tx_count']} | Total Amount: {row['total_amount']}"
            )
            # In real scenario, you'd call an API or send an email
            logging.warning(alert_msg)

    (
        risk_df
        .writeStream
        .foreachBatch(alert_batch_function)
        .start()
    )

# -----------------------------------------------------------------------------
# Main Process
# -----------------------------------------------------------------------------
def process_high_volume_transactions(
    bootstrap_servers: str = "kafka:9092",
    input_topic: str = "raw_transactions",
    output_topic: str = "risk_alerts",
    risk_checkpoint_path: str = "/checkpoints/risk_alerts",
    data_lake_path: str = "transactions_data_lake",
    data_lake_checkpoint_path: str = "/checkpoints/data_lake"
) -> None:
    """
    Main entry point that orchestrates the entire flow:
      1. Read from Kafka
      2. Parse and Validate transactions
      3. Persist valid transactions to data lake
      4. Detect high-risk transactions
      5. Publish risk alerts to Kafka
      6. Send additional alerts to external services (optional)

    :param bootstrap_servers: Kafka broker(s).
    :param input_topic: Kafka topic with raw transactions.
    :param output_topic: Kafka topic where risk alerts are published.
    :param risk_checkpoint_path: Filesystem path for checkpoint data for risk alerts.
    :param data_lake_path: Path where all valid transactions will be stored.
    :param data_lake_checkpoint_path: Filesystem path for checkpoint data for data lake writes.
    :return: None
    """
    logging.info("Starting high-volume transaction processing job.")
    spark = get_spark_session()
    schema = get_transaction_schema()

    try:
        # 1. Read raw data from Kafka
        raw_df = read_raw_transactions(
            spark, 
            bootstrap_servers=bootstrap_servers,
            input_topic=input_topic
        )

        # 2. Parse and validate data
        parsed_df = parse_transactions(raw_df, schema)
        validated_df = validate_transactions(parsed_df)

        # 3. Persist validated transactions to data lake
        lake_query = persist_transactions_to_data_lake(
            validated_df,
            output_path=data_lake_path,
            checkpoint_path=data_lake_checkpoint_path,
            output_mode="append",
            trigger_interval="1 minute"
        )

        # 4. Detect high-risk transactions
        risk_df = detect_high_risk_transactions(validated_df)

        # 5. Publish high-risk transactions to Kafka
        risk_query = write_high_risk_transactions(
            risk_df,
            bootstrap_servers=bootstrap_servers,
            output_topic=output_topic,
            checkpoint_path=risk_checkpoint_path
        )

        # 6. (Optional) Send alerts to external systems
        send_alerts_if_needed(risk_df)

        logging.info("All streaming queries have started. Waiting for termination...")
        # Optionally wait for streams to end (this is blocking).
        # In many production setups, you let them run indefinitely.
        # risk_query.awaitTermination()
        # lake_query.awaitTermination()

    except Exception as e:
        logging.error("Failed to process high-volume transactions: %s", e, exc_info=True)
        raise
