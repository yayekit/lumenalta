from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    from_json, 
    col, 
    window, 
    count, 
    sum
)
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    DecimalType, 
    TimestampType
)

# Define schema for transaction data
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("user_id", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True)
])

def process_high_volume_transactions(spark: SparkSession) -> StreamingQuery:
    """
    Reads transaction data from a Kafka topic, identifies high-volume or
    large-amount transactions, and publishes risk alerts to another Kafka topic.

    :param spark: Active SparkSession to use for streaming
    :return: The streaming query handling the risk alerts
    """
    
    # 1. Read streaming data from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "raw_transactions")
        .load()
    )

    # 2. Parse the raw JSON messages into structured columns
    parsed_stream = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
        )
        .select("data.*")  # Flatten out the nested structure
        .withWatermark("timestamp", "5 minutes")
    )

    # 3. Aggregate and flag suspicious patterns in transactions
    risk_analysis = (
        parsed_stream
        .groupBy(window(col("timestamp"), "1 minute"), col("user_id"))
        .agg(
            count("*").alias("tx_count"),
            sum(col("amount")).alias("total_amount")
        )
        .where("tx_count > 10 OR total_amount > 10000")
    )

    # 4. Write high-risk transactions to a Kafka topic
    query = (
        risk_analysis
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", "risk_alerts")
        .option("checkpointLocation", "/checkpoints/risk_alerts")
        .start()
    )

    return query
