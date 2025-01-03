import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from config import Config

def persist_transactions_to_data_lake(
    parsed_df: DataFrame,
    output_path: str = Config.DATA_LAKE_PATH,
    checkpoint_path: str = Config.CHECKPOINT_PATH_LAKE,
    output_mode: str = "append",
    trigger_interval: str = "1 minute"
) -> StreamingQuery:
    """
    Persists validated transactions to a data lake (S3, HDFS, or local path).
    """
    logging.info("Persisting validated transactions to data lake path: %s", output_path)
    query = (
        parsed_df
        .writeStream
        .outputMode(output_mode)
        .format("parquet")  # or "delta"
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query
