import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, count, sum as sum_
from config import Config

def augment_with_user_profile(transactions_df: DataFrame) -> DataFrame:
    """
    Example of how you might join external data (user risk profiles)
    from a database or an external system. 
    For demonstration, we'll assume we have a static DataFrame or UDF.
    """
    logging.info("Augmenting transactions with user profile data.")
    
    # In production, you might do:
    # user_profiles_df = spark.read.format("jdbc")...
    # For now, a mock example:
    from pyspark.sql.functions import lit
    user_profiles_df = transactions_df.select("user_id").distinct().withColumn("risk_category", lit("HIGH"))
    
    # Join on user_id
    joined_df = transactions_df.join(user_profiles_df, on="user_id", how="left")
    return joined_df

def detect_high_risk_transactions(transactions_df: DataFrame) -> DataFrame:
    """
    Detects high-risk transactions based on dynamic thresholds 
    (either from config or augmented data).
    """
    logging.info(
        "Detecting high-risk transactions with thresholds: tx_count > %d or total_amount > %.2f, window=%s",
        Config.TX_COUNT_THRESHOLD,
        Config.AMOUNT_THRESHOLD,
        Config.WINDOW_DURATION
    )

    # Group by window and user, then aggregate
    risk_df = (
        transactions_df
        .groupBy(window(col("timestamp"), Config.WINDOW_DURATION), col("user_id"))
        .agg(
            count("*").alias("tx_count"),
            sum_("amount").alias("total_amount")
        )
        .where(
            (col("tx_count") > Config.TX_COUNT_THRESHOLD) |
            (col("total_amount") > Config.AMOUNT_THRESHOLD)
        )
    )
    return risk_df
