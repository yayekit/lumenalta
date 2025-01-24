from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import datetime

def create_optimize_spark_session(config_dict: dict) -> SparkSession:
	"""creating an optimized spark session for large data processing"""
	builder = SparkSession.builder
	builder = builder.appName(config_dict.get("app_name", "KafkaSparkPipeline")) \
        .config("spark.sql.shuffle.partitions", config_dict.get("shuffle_partitions", "800")) \
        .config("spark.executor.memory", config_dict.get("executor_memory", "8g")) \
        .config("spark.driver.memory", config_dict.get("driver_memory", "4g")) \
        .config("spark.executor.cores", config_dict.get("executor_cores", "4")) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", config_dict.get("min_executors", "1")) \
        .config("spark.dynamicAllocation.maxExecutors", config_dict.get("max_executors", "10")) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", config_dict.get("initial_partitions", "200"))
	
	
	return builder.getOrCreate()

def define_data_schema():
	"""defines a sample data schema for incoming kafka data"""
	schema = StructType([
		StructField("event_id", StringType(), True),
		StructField("user_id", IntegerType(), True),
		StructField("timestamp", TimestampType(), True),
		StructField("event_type", StringType(), True),
		StructField("value", DoubleType(), True),
		StructField("location", StringType(), True),
		StructField("device_type", StringType(), True),
		StructField("session_id", StringType(), True)
	])
	return schema

def read_from_kafka(spark: SparkSession, kafka_config: dict, schema: StructType):
    """Reads data from Kafka topic"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("subscribe", kafka_config["topic"]) \
        .option("startingOffsets", kafka_config.get("starting_offsets", "earliest")) \
        .option("failOnDataLoss", "false") \
		.option("kafka.security.protocol", kafka_config.get("security_protocol","")) \
        .option("kafka.sasl.mechanism", kafka_config.get("sasl_mechanism","")) \
        .option("kafka.sasl.plain.username", kafka_config.get("sasl_plain_username",""))\
        .option("kafka.sasl.plain.password", kafka_config.get("sasl_plain_password",""))\
        .load()

    # Deserialize the Kafka value using the defined schema
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(F.from_json("value", schema).alias("data")).select("data.*")

    return df


def process_data(df):
	"""performs simple data transformation like adding a date column"""
	df = df.withColumn("event_date", F.to_date("timestamp"))
	df = df.withColumn("event_hour", F.hour("timestamp"))
	return df
	
def write_to_sink(df, sink_config: dict):
    """writes the stream to sink"""
    query = df.writeStream \
        .outputMode(sink_config.get("output_mode", "append")) \
        .format(sink_config.get("format", "parquet")) \
        .option("checkpointLocation", sink_config["checkpoint_location"]) \
        .option("path", sink_config["path"]) \
        .partitionBy("event_date", "event_hour")\
        .start()
    return query


if __name__ == '__main__':
	# Configuration for the pipeline
	config = {
		"app_name":"kafka_500gb_pipeline",
		"shuffle_partitions": "1000",
		"executor_memory": "12g",
		"driver_memory": "6g",
		"executor_cores": "6",
		"min_executors": "2",
		"max_executors": "15",
		"initial_partitions": "400"
		}
	kafka_config = {
		"bootstrap_servers": "your_kafka_brokers:9092", # Replace with your brokers
		"topic": "your_topic",  # Replace with your topic
		"starting_offsets": "latest", # or 'earliest'
        # Add below lines if your kafka cluster need authentication
        #"security_protocol" : "SASL_SSL",
        #"sasl_mechanism" : "PLAIN",
        #"sasl_plain_username" : "your_user",
        #"sasl_plain_password" : "your_password"
	}
	sink_config = {
		"format": "parquet",
		"path": "/path/to/your/output", # Replace with your output path
		"checkpoint_location": "/path/to/your/checkpoint", # Replace with your checkpoint path
		"output_mode":"append" # or "complete" if you need to aggregate the data
	}
	
	# Create an optimized spark session
	spark = create_optimize_spark_session(config)

	# Define the data schema
	schema = define_data_schema()

	# Read data from kafka
	kafka_df = read_from_kafka(spark, kafka_config, schema)
	
	# Transform data 
	transformed_df = process_data(kafka_df)
	
	#write stream to sink
	query = write_to_sink(transformed_df, sink_config)
	
	query.awaitTermination()