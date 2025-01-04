import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def _alert_on_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Handles alert logic for each micro-batch in the stream.

    :param batch_df: The DataFrame corresponding to this micro-batch.
    :param batch_id: The unique ID for this micro-batch.
    """
    records = batch_df.collect()
    for row in records:
        alert_msg = (
            f"[ALERT] High-risk transaction detected: "
            f"User={row['user_id']} | Count={row['tx_count']} | Amount={row['total_amount']}"
        )
        # Log the alert message. 
        # In a real scenario, you might send an HTTP request, Slack message, email, etc.
        logging.warning(alert_msg)

        # Example of pushing a metric to a monitoring system (pseudo-code):
        # pseudo_push_metric("high_risk_transactions_count", 1)


def start_high_risk_alert_stream(risk_df: DataFrame) -> StreamingQuery:
    """
    Starts a streaming job that sends alerts when high-risk transactions are detected.
    Optionally, you could push metrics to your monitoring system here as well.

    :param risk_df: A streaming DataFrame of transactions already identified as high-risk.
    :return: A StreamingQuery that tracks the state of this alerting stream.
    """
    return (
        risk_df
        .writeStream
        .foreachBatch(_alert_on_batch)
        .start()
    )
