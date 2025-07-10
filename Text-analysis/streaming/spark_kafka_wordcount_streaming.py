from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, lower, window, current_timestamp
from pyspark.sql.types import StructType, StringType

# ----------------- Configurations -----------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "reviews"
S3_OUTPUT_PATH = "s3a://txt-analysis-results/wordcount-stream/"
S3_CHECKPOINT_PATH = "s3a://txt-analysis-results/wordcount-checkpoints/"

# ----------------- Spark Session -----------------
spark = SparkSession.builder \
    .appName("KafkaWordCountToS3") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ----------------- Schema -----------------
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# ----------------- Read Kafka Stream -----------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# ----------------- Parse JSON and Tokenize -----------------
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.review") \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("word", explode(split(lower(col("review")), "\\s+")))

# ----------------- Filter Stop Words -----------------
STOP_WORDS = ["the", "is", "in", "and", "a", "to", "of"]
df_filtered = df.filter(~col("word").isin(STOP_WORDS))

# ----------------- Apply Watermark and Windowed Aggregation -----------------
word_counts = df_filtered \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "60 seconds", "30 seconds"),
        col("word")
    ).count()

# ----------------- Write to S3 -----------------
query = word_counts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", S3_OUTPUT_PATH) \
    .option("checkpointLocation", S3_CHECKPOINT_PATH) \
    .trigger(processingTime="60 seconds") \
    .start()

print("✅ Streaming top word counts to S3 every 60 seconds...")

query.awaitTermination()

