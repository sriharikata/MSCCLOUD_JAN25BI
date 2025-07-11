from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, explode, split, window

# ✅ Define schema for incoming Kafka JSON messages
schema = StructType() \
    .add("review", StringType()) \
    .add("sentiment", StringType())

# ✅ Create Spark Session
spark = SparkSession.builder \
    .appName("SparkKafkaSlidingWindowDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\n✅ Spark Kafka Sliding Window Consumer running for CA demo...\n")

# ✅ Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reviews") \
    .option("startingOffsets", "earliest") \
    .load()

# ✅ Parse JSON, keep Kafka message timestamp
parsed = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema).alias("data"), col("timestamp")) \
    .select("data.review", "timestamp")

# ✅ Tokenize reviews into words
words = parsed.select(
    explode(split(col("review"), r"\W+")).alias("word"),
    col("timestamp").alias("ts")
).where("word != ''")

# Smaller window for faster updates
windowed = words.groupBy(
    window(col("ts"), "30 seconds", "10 seconds"),
    col("word")
).count().orderBy(col("count").desc())

# Stream output with faster trigger
query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

