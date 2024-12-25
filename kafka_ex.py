# Real-time transaction monitoring pipeline
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql.streaming import StreamingQuery

# 1. Kafka Consumer getting real-time transactions
consumer = KafkaConsumer(
    'raw_transactions',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 2. Spark Streaming processing these events
def process_transactions():
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_transactions") \
        .load()
    
    # Here's where you'd detect suspicious patterns
    risk_df = streaming_df \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window("timestamp", "1 minute"),
            "user_id"
        ) \
        .agg(
            count("*").alias("tx_count"),
            sum("amount").alias("total_amount")
        ) \
        .where("tx_count > 10 OR total_amount > 10000")  # Suspicious activity

    # Send alerts to another Kafka topic
    query = risk_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "risk_alerts") \
        .start()