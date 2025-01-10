def estimate_parquet_storage(df):
    # Rough estimate of parquet storage size
    sample_size = min(1000, df.count())
    sample = df.limit(sample_size)

    # Write a sample and measure it
    sample.write.parquet("tmp/sample", mode="overwrite")
    sample_size_bytes = sum(os.path.getsize(os.path.join(root, file)) 
                           for root, _, files in os.walk("tmp/sample") 
                           for file in files)
    
    # Extrapolate to full dataset
    estimated_size = (sample_size_bytes / sample_size) * df.count()
    return estimated_size
    
