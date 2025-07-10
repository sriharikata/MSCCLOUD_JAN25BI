from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, current_timestamp, window

# Configurations
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "reviews"
S3_OUTPUT_PATH = "s3a://txt-analysis-results/sentiment-stream/"
S3_CHECKPOINT_PATH = "s3a://txt-analysis-results/sentiment-checkpoints/"

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaSentimentCountToS3") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# Read Kafka Stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.review", "data.sentiment") \
    .withColumn("timestamp", current_timestamp())

# Apply watermark and windowed aggregation
sentiment_counts = df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "30 seconds"),  # adjustable window
        col("sentiment")
    ) \
    .count()

# Write to S3 as Parquet
query = sentiment_counts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", S3_OUTPUT_PATH) \
    .option("checkpointLocation", S3_CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Streaming windowed sentiment counts to S3 every 10 seconds...")

query.awaitTermination()

