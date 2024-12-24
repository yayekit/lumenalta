def build_medallion_pipeline():
    # bronze: raw ingestion
    bronze_df = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bronze")
        .load())