from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, explode, split, lower, window, count, desc

# ---------------- CONFIGURATION ----------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "reviews"
STOP_WORDS = ["the", "is", "in", "and", "a", "to", "of", "for", "on", "with", "at"]

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder \
    .appName("KafkaSlidingWindowWordCountWithSentiment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------- SCHEMA ----------------
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# ---------------- READ KAFKA STREAM ----------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ---------------- PARSE JSON ----------------
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.review", "data.sentiment")

# ---------------- TOKENIZE, CLEAN, FILTER ----------------
words_df = parsed_df.select(
    explode(split(lower(col("review")), "\\W+")).alias("word"),
    col("sentiment")
).where(~col("word").isin(STOP_WORDS)).where(col("word") != "")

# ---------------- SLIDING WINDOW WORD COUNT ----------------
windowed_counts = words_df.withWatermark("sentiment", "1 minute") \
    .groupBy(
        window(col("sentiment"), "60 seconds", "20 seconds"),
        col("word")
    ).agg(count("word").alias("count")) \
    .orderBy(desc("count"))

# ---------------- SENTIMENT DISTRIBUTION ----------------
sentiment_counts = parsed_df.groupBy("sentiment").count()

# ---------------- WRITE TO CONSOLE ----------------
query_words = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 10) \
    .start()

query_sentiments = sentiment_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("✅ Spark Kafka Streaming with Sliding Window Word Count & Sentiment Distribution running...")

query_words.awaitTermination()
query_sentiments.awaitTermination()

