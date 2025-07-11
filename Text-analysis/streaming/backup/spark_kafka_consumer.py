from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col

# Define the schema of the incoming JSON data
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaReviewConsumer") \
    .getOrCreate()

# Read stream from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reviews") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka message
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.review", "data.sentiment")

# Count sentiment values
sentiment_counts = df.groupBy("sentiment").count()

# Output results to console
query = sentiment_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

