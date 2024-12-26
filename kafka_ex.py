from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import from_json, col, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

# Define schema for our transaction data
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DecimalType(10,2)),
    StructField("timestamp", TimestampType())
])

def process_high_volume_transactions():
    # Kafka is our source of streaming data
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_transactions") \
        .load()

    # Now Spark takes over for heavy processing
    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "5 minutes")

    # Complex aggregations - this is where Spark shines
    risk_analysis = parsed_stream \
        .groupBy(
            window("timestamp", "1 minute"),
            "user_id"
        ) \
        .agg(
            count("*").alias("tx_count"),
            sum("amount").alias("total_amount")
        ) \
        .where("tx_count > 10 OR total_amount > 10000")

    # Output high-risk transactions back to Kafka for other systems
    query = risk_analysis.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "risk_alerts") \
        .option("checkpointLocation", "/checkpoints/risk_alerts") \
        .start()

    return query