import os

class Config:
    """
    Centralized configuration parameters.
    We retrieve environment variables or default to known values.
    """
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw_transactions")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "risk_alerts")
    
    # Checkpoints
    CHECKPOINT_PATH_RISK = os.getenv("CHECKPOINT_PATH_RISK", "/checkpoints/risk_alerts")
    CHECKPOINT_PATH_LAKE = os.getenv("CHECKPOINT_PATH_LAKE", "/checkpoints/data_lake")

    # Data lake output
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "transactions_data_lake")

    # Thresholds (could also come from a database or microservice)
    TX_COUNT_THRESHOLD = int(os.getenv("TX_COUNT_THRESHOLD", 10))
    AMOUNT_THRESHOLD = float(os.getenv("AMOUNT_THRESHOLD", 10000.0))

    # Window / Watermark
    WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
    WATERMARK_DURATION = os.getenv("WATERMARK_DURATION", "5 minutes")

    # Spark
    APP_NAME = os.getenv("APP_NAME", "HighVolumeTransactionAnalysis")
