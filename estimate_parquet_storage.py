import os
import shutil
import tempfile
import logging
from pyspark.sql import DataFrame

def estimate_parquet_storage(df: DataFrame, sample_size_rows: int = 1000) -> int:
    """
    Estimates the storage size in bytes of a DataFrame when written to Parquet format.

    This is a rough estimate based on a sample of the DataFrame. The actual size
    may vary depending on data compression, schema complexity, and other factors.

    Args:
        df: The DataFrame to estimate storage size for.
        sample_size_rows: The number of rows to sample for estimation. Defaults to 1000.

    Returns:
        int: Estimated storage size in bytes. Returns -1 if estimation fails.
    """
    temp_dir = tempfile.mkdtemp()
    sample_parquet_path = os.path.join(temp_dir, "sample")
    estimated_size = -1 # Initialize to -1 to indicate failure if it occurs

    try:
        logging.info(f"Estimating Parquet storage size using a sample of {min(sample_size_rows, df.count())} rows.")
        sample_size = min(sample_size_rows, df.count())
        sample = df.limit(sample_size)

        # Write a sample and measure it
        sample.write.parquet(sample_parquet_path, mode="overwrite")

        sample_size_bytes = 0
        for root, _, files in os.walk(sample_parquet_path):
            for file in files:
                sample_size_bytes += os.path.getsize(os.path.join(root, file))

        if sample_size > 0: # Avoid division by zero
            # Extrapolate to full dataset
            estimated_size = int((sample_size_bytes / sample_size) * df.count())
            logging.info(f"Estimated Parquet storage size: {estimated_size} bytes.")
        else:
            logging.warning("DataFrame is empty. Cannot estimate storage size.")
            estimated_size = 0 # Or handle empty DataFrame case as needed

    except Exception as e:
        logging.error(f"Error estimating Parquet storage size: {e}")
        estimated_size = -1 # Indicate failure
    finally:
        try:
            shutil.rmtree(temp_dir) # Clean up the temporary directory
            logging.debug(f"Cleaned up temporary directory: {temp_dir}")
        except OSError as e:
            logging.warning(f"Error cleaning up temporary directory {temp_dir}: {e}")

    return estimated_size