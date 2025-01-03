from pyspark.sql.types import (
    StructType, StructField,
    StringType, DecimalType, TimestampType
)

def get_transaction_schema() -> StructType:
    """
    Defines the schema for the transaction data.
    """
    return StructType([
        StructField("transaction_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("amount", DecimalType(10, 2), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True)
    ])
