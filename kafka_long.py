import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException

def create_spark_session(app_name: str = "HighVolumeTransactionProcessor") -> SparkSession:
    """
    Create or retrieve an existing SparkSession.

    :param app_name: Name for the Spark application.
    :return: A SparkSession instance.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def define_schema() -> StructType:
    """
    Define schema for transaction data.

    :return: A StructType object representing the schema.
    """
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("timestamp", TimestampType(), True)
    ])


def read_stream_from_kafka(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    input_topic: str,
    schema: StructType,
    watermark_threshold: str = "5 minutes"
):
    """
    Read streaming data from Kafka and parse it using the provided schema.

    :param spark: SparkSession object.
    :param kafka_bootstrap_servers: Kafka bootstrap servers, e.g. "kafka:9092".
    :param input_topic: Kafka topic to read from.
    :param schema: Schema for parsing the incoming JSON data.
    :param watermark_threshold: Watermark threshold for late data.
    :return: A parsed DataFrame representing the streaming data.
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withWatermark("timestamp", watermark_threshold)
    )

    return parsed_stream


def process_risk_analysis(parsed_stream):
    """
    Identify high-volume or high-amount transactions using Spark aggregations.

    :param parsed_stream: Parsed DataFrame from Kafka source.
    :return: DataFrame with potential risk alerts.
    """
    risk_analysis = (
        parsed_stream
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("user_id")
        )
        .agg(
            count("*").alias("tx_count"),
            spark_sum("amount").alias("total_amount")
        )
        .where("tx_count > 10 OR total_amount > 10000")
    )
    return risk_analysis


def write_stream_to_kafka(
    df,
    kafka_bootstrap_servers: str,
    output_topic: str,
    checkpoint_location: str
) -> StreamingQuery:
    """
    Write the result DataFrame back to a Kafka topic.

    :param df: DataFrame to write out.
    :param kafka_bootstrap_servers: Kafka bootstrap servers.
    :param output_topic: Kafka topic to write to.
    :param checkpoint_location: HDFS/S3 path for checkpointing.
    :return: A StreamingQuery object.
    """
    query = (
        df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    return query


def process_high_volume_transactions(
    kafka_bootstrap_servers="kafka:9092",
    input_topic="raw_transactions",
    output_topic="risk_alerts",
    checkpoint_location="/checkpoints/risk_alerts"
) -> StreamingQuery:
    """
    Orchestrates the entire workflow of reading, processing, and writing
    high-volume transactions. Returns a StreamingQuery which can be awaited
    or monitored.

    :param kafka_bootstrap_servers: Kafka bootstrap servers.
    :param input_topic: Topic to read transactions from.
    :param output_topic: Topic to write risk alerts to.
    :param checkpoint_location: Path to save checkpoints.
    :return: The active StreamingQuery object.
    """
    spark = create_spark_session()
    transaction_schema = define_schema()

    # Read from Kafka
    parsed_stream = read_stream_from_kafka(
        spark=spark,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        input_topic=input_topic,
        schema=transaction_schema
    )

    # Process stream for risk analysis
    risk_analysis_df = process_risk_analysis(parsed_stream)

    # Write to Kafka
    query = write_stream_to_kafka(
        df=risk_analysis_df,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        output_topic=output_topic,
        checkpoint_location=checkpoint_location
    )

    return query


def main():
    """
    Main entry point to run the high-volume transaction streaming pipeline.
    Includes basic error handling for the streaming query.
    """
    try:
        query = process_high_volume_transactions(
            kafka_bootstrap_servers="kafka:9092",
            input_topic="raw_transactions",
            output_topic="risk_alerts",
            checkpoint_location="/checkpoints/risk_alerts"
        )
        query.awaitTermination()
    except StreamingQueryException as e:
        # Log and handle the streaming failure
        print(f"Streaming query failed: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        print("Interrupted by user, shutting down...")
        sys.exit(0)
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
