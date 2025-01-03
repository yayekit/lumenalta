import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from config import Config
from pyspark.sql.functions import col

def read_raw_transactions(spark, 
                          bootstrap_servers: str = Config.KAFKA_BOOTSTRAP_SERVERS,
                          input_topic: str = Config.INPUT_TOPIC) -> DataFrame:
    """
    Reads raw streaming transactions from Kafka.
    """
    logging.info("Reading raw transactions from Kafka topic: %s", input_topic)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .load()
    )

def write_high_risk_transactions(risk_df: DataFrame,
                                 bootstrap_servers: str = Config.KAFKA_BOOTSTRAP_SERVERS,
                                 output_topic: str = Config.OUTPUT_TOPIC,
                                 checkpoint_path: str = Config.CHECKPOINT_PATH_RISK) -> StreamingQuery:
    """
    Writes the high-risk transactions to a Kafka topic.
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
