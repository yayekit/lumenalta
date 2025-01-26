from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import datetime
import logging

def create_optimize_spark_session(config_dict: dict) -> SparkSession:
    """
    Creates an optimized SparkSession for large data processing.

    This function configures SparkSession with settings tailored for handling large datasets,
    including shuffle partitions, memory allocation, dynamic allocation, and adaptive query execution.

    Args:
        config_dict (dict): A dictionary containing Spark configuration parameters.
                             Expected keys include:
                               - "app_name" (str, optional): Application name. Defaults to "KafkaSparkPipeline".
                               - "shuffle_partitions" (str, optional): Number of shuffle partitions. Defaults to "800".
                               - "executor_memory" (str, optional): Executor memory. Defaults to "8g".
                               - "driver_memory" (str, optional): Driver memory. Defaults to "4g".
                               - "executor_cores" (str, optional): Executor cores. Defaults to "4".
                               - "min_executors" (str, optional): Minimum executors for dynamic allocation. Defaults to "1".
                               - "max_executors" (str, optional): Maximum executors for dynamic allocation. Defaults to "10".
                               - "initial_partitions" (str, optional): Initial number of partitions for adaptive execution. Defaults to "200".
                               - "memory_fraction" (str, optional): Fraction of JVM heap space used for execution and storage memory. Defaults to "0.6".
                               - "storage_fraction" (str, optional): Fraction of `spark.memory.fraction` used for storage memory. Defaults to "0.5".

    Returns:
        SparkSession: An optimized SparkSession.
    """
    builder = SparkSession.builder
    builder = builder.appName(config_dict.get("app_name", "KafkaSparkPipeline"))

    # Core performance tuning configurations
    builder = builder.config("spark.sql.shuffle.partitions", config_dict.get("shuffle_partitions", "1000")) # Increased default shuffle partitions for larger datasets
    builder = builder.config("spark.executor.memory", config_dict.get("executor_memory", "12g")) # Increased default executor memory
    builder = builder.config("spark.driver.memory", config_dict.get("driver_memory", "6g"))   # Increased default driver memory
    builder = builder.config("spark.executor.cores", config_dict.get("executor_cores", "6"))     # Increased default executor cores

    # Dynamic allocation for resource optimization
    builder = builder.config("spark.dynamicAllocation.enabled", "true")
    builder = builder.config("spark.dynamicAllocation.minExecutors", config_dict.get("min_executors", "2")) # Adjusted min executors
    builder = builder.config("spark.dynamicAllocation.maxExecutors", config_dict.get("max_executors", "15")) # Adjusted max executors

    # Adaptive Query Execution for optimization during runtime
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", config_dict.get("initial_partitions", "400")) # Adjusted initial partitions for AQE

    # Memory management tuning (optional, adjust based on workload)
    builder = builder.config("spark.memory.fraction", config_dict.get("memory_fraction", "0.6")) # Fraction of heap for Spark execution/storage
    builder = builder.config("spark.memory.storageFraction", config_dict.get("storage_fraction", "0.5")) # Fraction of spark.memory.fraction for storage

    spark_session = builder.getOrCreate()
    logging.info("Optimized SparkSession created with configurations: %s", config_dict)
    return spark_session

def define_data_schema() -> StructType:
    """
    Defines the schema for incoming Kafka data.

    This schema is a sample and should be adjusted based on the actual data format
    expected from the Kafka topic.

    Returns:
        StructType: The defined data schema.
    """
    schema = StructType([
        StructField("event_id", StringType(), True, metadata={"description": "Unique event identifier"}),
        StructField("user_id", IntegerType(), True, metadata={"description": "Identifier of the user"}),
        StructField("timestamp", TimestampType(), True, metadata={"description": "Timestamp of the event"}),
        StructField("event_type", StringType(), True, metadata={"description": "Type of event"}),
        StructField("value", DoubleType(), True, metadata={"description": "Numeric value associated with the event"}),
        StructField("location", StringType(), True, metadata={"description": "Geographical location of the event"}),
        StructField("device_type", StringType(), True, metadata={"description": "Type of device used"}),
        StructField("session_id", StringType(), True, metadata={"description": "User session identifier"})
    ])
    logging.info("Data schema defined: %s", schema.json())
    return schema

def read_from_kafka(spark: SparkSession, kafka_config: dict, schema: StructType):
    """Reads data from Kafka topic.

    Args:
        spark (SparkSession): The SparkSession.
        kafka_config (dict): Kafka configuration parameters, including:
                             - "bootstrap_servers" (str, required)
                             - "topic" (str, required)
                             - "starting_offsets" (str, optional, default "earliest")
                             - "security_protocol" (str, optional)
                             - "sasl_mechanism" (str, optional)
                             - "sasl_plain_username" (str, optional)
                             - "sasl_plain_password" (str, optional)
        schema (StructType): The schema to apply to the Kafka message value.

    Returns:
        DataFrame: DataFrame representing the data read from Kafka.
    """
    logging.info("Reading from Kafka topic: %s", kafka_config.get("topic"))
    kafka_reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("subscribe", kafka_config["topic"]) \
        .option("startingOffsets", kafka_config.get("starting_offsets", "earliest")) \
        .option("failOnDataLoss", "false")

    # Add security configurations if provided
    if kafka_config.get("security_protocol"):
        kafka_reader = kafka_reader.option("kafka.security.protocol", kafka_config["security_protocol"])
    if kafka_config.get("sasl_mechanism"):
        kafka_reader = kafka_reader.option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    if kafka_config.get("sasl_plain_username"):
        kafka_reader = kafka_reader.option("kafka.sasl.plain.username", kafka_config["sasl_plain_username"])
    if kafka_config.get("sasl_plain_password"):
        kafka_reader = kafka_reader.option("kafka.sasl.plain.password", kafka_config["sasl_plain_password"])

    df = kafka_reader.load()

    # Deserialize the Kafka value using the defined schema
    df = df.selectExpr("CAST(value AS STRING)")
    try:
        df = df.select(F.from_json("value", schema).alias("data")).select("data.*")
    except Exception as e:
        logging.error("Error parsing JSON from Kafka value: %s. Please ensure Kafka messages are valid JSON and schema matches.", e)
        raise # Re-raise the exception after logging
    logging.info("Successfully read and deserialized data from Kafka topic: %s", kafka_config.get("topic"))
    return df


def process_data(df):
    """Performs data transformations: adds date and hour columns from timestamp.

    Args:
        df (DataFrame): Input DataFrame with a 'timestamp' column.

    Returns:
        DataFrame: DataFrame with added 'event_date' and 'event_hour' columns.
    """
    df = df.withColumn("event_date", F.to_date("timestamp"))
    df = df.withColumn("event_hour", F.hour("timestamp"))
    logging.info("Data processing completed: added 'event_date' and 'event_hour' columns.")
    return df

def write_to_sink(df, sink_config: dict):
    """Writes the stream to sink with partitioning and checkpointing.

    Args:
        df (DataFrame): DataFrame to write to sink.
        sink_config (dict): Sink configuration parameters, including:
                             - "format" (str, optional, default "parquet")
                             - "path" (str, required)
                             - "checkpoint_location" (str, required)
                             - "output_mode" (str, optional, default "append")
                             - "partition_columns" (list, optional, default ["event_date", "event_hour"])

    Returns:
        StreamingQuery: The streaming query object.
    """
    partition_cols = sink_config.get("partition_columns", ["event_date", "event_hour"]) # Default partition columns
    logging.info("Writing stream to sink in format: %s, path: %s, partitioned by: %s", sink_config.get("format", "parquet"), sink_config["path"], partition_cols)
    query = df.writeStream \
        .outputMode(sink_config.get("output_mode", "append")) \
        .format(sink_config.get("format", "parquet")) \
        .option("checkpointLocation", sink_config["checkpoint_location"]) \
        .option("path", sink_config["path"])

    if partition_cols: # Only partition if partition columns are specified
        query = query.partitionBy(*partition_cols)

    query = query.start()
    logging.info("Streaming query started to write data to sink.")
    return query


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Configuration for the pipeline - consider moving to config.py or external config
    config = {
        "app_name":"kafka_500gb_pipeline",
        "shuffle_partitions": "2000", # Increased for 500GB scale
        "executor_memory": "20g",     # Increased for 500GB scale
        "driver_memory": "10g",      # Increased for 500GB scale
        "executor_cores": "8",        # Increased for 500GB scale
        "min_executors": "3",         # Adjusted for 500GB scale
        "max_executors": "20",        # Adjusted for 500GB scale
        "initial_partitions": "800",  # Increased for 500GB scale
        "memory_fraction": "0.7",     # Adjusted memory fraction
        "storage_fraction": "0.6"    # Adjusted storage fraction
    }
    kafka_config = {
        "bootstrap_servers": "your_kafka_brokers:9092", # Replace with your brokers
        "topic": "your_topic",  # Replace with your topic
        "starting_offsets": "latest", # or 'earliest'
        # Add below lines if your kafka cluster need authentication
        #"security_protocol" : "SASL_SSL",
        #"sasl_mechanism" : "PLAIN",
        #"sasl_plain_username" : "your_user",
        #"sasl_plain_password" : "your_password"
    }
    sink_config = {
        "format": "parquet",
        "path": "/path/to/your/output", # Replace with your output path
        "checkpoint_location": "/path/to/your/checkpoint", # Replace with your checkpoint path
        "output_mode":"append", # or "complete" if you need to aggregate the data
        #"partition_columns": ["event_date", "event_hour", "event_type"] # Example of adding more partition columns
    }

    try:
        # Create an optimized spark session
        spark = create_optimize_spark_session(config)

        # Define the data schema
        schema = define_data_schema()

        # Read data from kafka
        kafka_df = read_from_kafka(spark, kafka_config, schema)

        # Transform data
        transformed_df = process_data(kafka_df)

        #write stream to sink
        query = write_to_sink(transformed_df, sink_config)

        query.awaitTermination()

    except Exception as e:
        logging.error("Pipeline encountered an error: %s", e, exc_info=True)