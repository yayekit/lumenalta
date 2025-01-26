from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from config import Config
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType  # Import StructType for schema
import logging

def read_raw_transactions(spark,
                          bootstrap_servers: str = Config.KAFKA_BOOTSTRAP_SERVERS,
                          input_topic: str = Config.INPUT_TOPIC,
                          starting_offsets: str = "latest", # Added startingOffsets option
                          value_schema: StructType = None # Added value_schema option
                          ) -> DataFrame:
    """
    Reads raw streaming transactions from Kafka.

    Args:
        spark: SparkSession.
        bootstrap_servers: Kafka bootstrap servers.
        input_topic: Kafka input topic.
        starting_offsets: Kafka starting offsets (e.g., "latest", "earliest"). Defaults to "latest".
        value_schema:  Schema for the Kafka message value (for deserialization).
                       If provided, the 'value' column will be parsed as JSON according to this schema.
                       If None, the 'value' column will be returned as a string.
    Returns:
        DataFrame: DataFrame representing the raw transactions stream.
    """
    logging.info(f"Reading raw transactions from Kafka topic: {input_topic}, startingOffsets: {starting_offsets}")
    kafka_reader = spark.readStream.format("kafka") \
                           .option("kafka.bootstrap.servers", bootstrap_servers) \
                           .option("subscribe", input_topic) \
                           .option("startingOffsets", starting_offsets)

    raw_df = kafka_reader.load()

    if value_schema is not None:
        # Deserialize JSON value if schema is provided
        return raw_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                     .select(col("key"), from_json(col("value"), value_schema).alias("value")) \
                     .select("key", "value.*") # Flatten the value struct
    else:
        return raw_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") # Return value as string if no schema


def write_high_risk_transactions(risk_df: DataFrame,
                                 bootstrap_servers: str = Config.KAFKA_BOOTSTRAP_SERVERS,
                                 output_topic: str = Config.OUTPUT_TOPIC,
                                 checkpoint_path: str = Config.CHECKPOINT_PATH_RISK,
                                 output_mode: str = "append" # Added output_mode option
                                 ) -> StreamingQuery:
    """
    Writes the high-risk transactions to a Kafka topic as JSON.

    Args:
        risk_df: DataFrame containing high-risk transactions.
        bootstrap_servers: Kafka bootstrap servers.
        output_topic: Kafka output topic.
        checkpoint_path: Checkpoint location for streaming query.
        output_mode: Output mode for the streaming write operation ("append", "complete", "update"). Defaults to "append".
    Returns:
        StreamingQuery: The streaming query object.
    """
    logging.info(f"Writing high-risk transactions to Kafka topic: {output_topic} with outputMode: {output_mode}")

    # Serialize DataFrame to JSON string before writing to Kafka value
    output_df = risk_df.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") # Assuming 'key' column exists or can be added

    query = (
        output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_path)
        .outputMode(output_mode)
        .start()
    )
    return query