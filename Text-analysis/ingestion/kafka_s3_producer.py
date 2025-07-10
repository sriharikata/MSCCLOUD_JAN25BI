from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaS3Producer") \
    .getOrCreate()

# Define S3 path
s3_path = "s3a://text-analysis1/IMDB Dataset.csv"
#s3_path = "s3a://text-analysis1/IMDB_Dataset_Expanded.csv"

# Read CSV from S3
df = spark.read.option("header", True).csv(s3_path)
print_interval = 100
# Function to send data to Kafka in parallel per partition
def send_partition(partition):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for i, row in enumerate(partition):
        if row['review'] and row['sentiment']:
            message = {
                "review": row['review'],
                "sentiment": row['sentiment']
            }
            if i % print_interval == 0:
                preview = (row['review'][:80] + '...') if len(row['review']) > 80 else row['review']
                print(f"\n=== Sending Kafka Message #{i} ===\nReview: \"{preview}\"\nSentiment: {row['sentiment']}\n=============================\n")
            producer.send("reviews", value=message)

    producer.flush()
    producer.close()

# Process each partition concurrently (better than collect())
df.foreachPartition(send_partition)

# Stop Spark session
spark.stop()

