import logging
from logging_setup import setup_logging
from spark_setup import get_spark_session
from kafka_io import read_raw_transactions, write_high_risk_transactions
from validation import parse_and_validate_transactions
from aggregator import augment_with_user_profile, detect_high_risk_transactions
from storage import persist_transactions_to_data_lake
from alerting import send_alerts_if_needed
from config import Config

def main():
    """
    Orchestrates the streaming pipeline:
      1. Read raw transactions from Kafka
      2. Parse & Validate
      3. (Optionally) Augment data with user profiles
      4. Persist to Data Lake
      5. Detect high-risk transactions
      6. Write high-risk to Kafka
      7. Send external alerts
    """
    # 1. Setup logging
    setup_logging()

    logging.info("Starting the High-Volume Transaction Processing Pipeline...")

    try:
        # 2. Spark Session
        spark = get_spark_session()

        # 3. Read from Kafka
        raw_df = read_raw_transactions(spark)

        # 4. Parse & Validate
        validated_df = parse_and_validate_transactions(raw_df)

        # 5. (Optional) Join with user profile data
        augmented_df = augment_with_user_profile(validated_df)

        # 6. Persist validated & augmented data to data lake
        lake_query = persist_transactions_to_data_lake(augmented_df)

        # 7. Detect high-risk transactions
        risk_df = detect_high_risk_transactions(augmented_df)

        # 8. Write high-risk events to Kafka
        risk_query = write_high_risk_transactions(risk_df)

        # 9. Send external alerts (Slack, email, etc.) and push metrics
        alert_query = send_alerts_if_needed(risk_df)

        logging.info("All streaming queries have started.")
        logging.info("Press Ctrl+C to terminate.")
        
        # Optionally block until termination. Otherwise, you can let them run indefinitely.
        # risk_query.awaitTermination()
        # lake_query.awaitTermination()
        # alert_query.awaitTermination()

    except Exception as e:
        logging.error("Pipeline encountered an error: %s", e, exc_info=True)
        raise

if __name__ == "__main__":
    main()
