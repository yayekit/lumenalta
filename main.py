import logging
from logging_setup import setup_logging
from spark_setup import get_spark_session
from kafka_io import KafkaIO
from validation import TransactionValidator
from aggregator import TransactionAggregator
from alerting import AlertingService
from storage import DataLakeWriter
from schema import get_kafka_value_schema

def main():
    """
    Main function to orchestrate the high-volume transaction processing pipeline.
    Sets up logging, initializes Spark, reads from Kafka, validates transactions,
    aggregates for risk detection, performs alerting, and persists data to the data lake.
    """
    setup_logging(logging.INFO) # Initialize logging
    logger = logging.getLogger(__name__)
    logger.info("Starting High-Volume Transaction Processing Pipeline...")

    try:
        spark = get_spark_session() # Get Spark Session

        kafka_io = KafkaIO(spark) # Initialize Kafka I/O handler
        validator = TransactionValidator() # Initialize Transaction Validator
        aggregator = TransactionAggregator() # Initialize Aggregator
        alerter = AlertingService() # Initialize Alerting Service
        data_lake_writer = DataLakeWriter() # Initialize Data Lake Writer


        # 1. Read raw transactions from Kafka
        kafka_value_schema = get_kafka_value_schema() # Schema for Kafka message value (JSON string)
        raw_transactions_df = kafka_io.read_transactions_from_kafka(kafka_value_schema)

        # 2. Parse and Validate Transactions
        validated_transactions_df = validator.process_transactions(raw_transactions_df)

        # 3. (Optional) Augment with User Profile Data - Placeholder Functionality
        augmented_transactions_df = aggregator.augment_with_user_profile(validated_transactions_df)

        # 4. Persist Validated Transactions to Data Lake (via Kafka)
        data_lake_kafka_query = kafka_io.write_data_lake_transactions_to_kafka(augmented_transactions_df)

        # 5. Detect High-Risk Transactions
        high_risk_transactions_df = aggregator.detect_high_risk_transactions(augmented_transactions_df)

        # 6. Alert on High-Risk Transactions
        alert_stream_query = alerter.start_alert_stream(high_risk_transactions_df)

        # 7. [For Direct Data Lake Write - Alternative, kept as comment for example - using Kafka Data Lake Stream is preferred in current version]
        # data_lake_query = data_lake_writer.write_to_data_lake(augmented_transactions_df) # Direct write to Data Lake


        logger.info("Pipeline setup complete, all streaming queries started.")
        logger.info("Application is running, waiting for stream termination...")

        # Await stream termination - choose which query to await on based on pipeline needs.
        # For example, await on the alert stream, or data lake stream, or main data processing stream.
        alert_stream_query.awaitTermination() # Awaiting alert stream termination for demonstration.

    except Exception as e:
        logger.error(f"Pipeline encountered a critical error: {e}", exc_info=True)
        raise # Re-raise exception to signal pipeline failure externally if needed


if __name__ == "__main__":
    main()