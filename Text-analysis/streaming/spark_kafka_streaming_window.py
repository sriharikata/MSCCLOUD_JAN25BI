from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, explode, split, window, current_timestamp

# Defining schema for incoming Kafka JSON messages
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# Creating Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamingSlidingWindow") \
    .getOrCreate()

# Reading streaming data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reviews") \
    .option("startingOffsets", "latest") \
    .load()

# Extracting value and apply schema
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.review")

# Tokenize words and assign current timestamp to each row
words = df.select(
    explode(split(col("review"), r"\W+")).alias("word"),
    current_timestamp().alias("ts")  
).where("word != ''")

# Sliding window word count: 10-second window sliding every 5 seconds
windowed_counts = words.groupBy(
    window(col("ts"), "10 seconds", "5 seconds"),
    col("word")
).count().orderBy(col("count").desc())

# Writing results to console in streaming mode
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

