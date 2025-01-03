import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from schema import get_transaction_schema
from config import Config
from pyspark.sql.functions import from_json

def parse_and_validate_transactions(raw_df: DataFrame) -> DataFrame:
    """
    Parses raw Kafka data into structured columns (using the transaction schema),
    applies watermarking, and filters out invalid transactions.
    """
    schema = get_transaction_schema()

    # Parse JSON
    parsed_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withWatermark("timestamp", Config.WATERMARK_DURATION)
    )
    logging.info("Parsing transactions with watermark: %s", Config.WATERMARK_DURATION)

    # Basic validations: remove negative amounts, ensure mandatory columns
    validated_df = (
        parsed_df
        .filter(col("amount") >= 0)
        .filter(col("transaction_id").isNotNull() & col("user_id").isNotNull())
    )
    logging.info("Validating transactions to remove invalid records.")
    return validated_df
