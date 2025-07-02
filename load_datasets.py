import os

# Set environment variables (optional if using spark-submit)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"] = "/opt/spark"

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("ReviewDataLoad") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Load datasets from S3
imdb_path = "s3a://text-analysis1/IMDB Dataset.csv"
amazon_path = "s3a://text-analysis1/amazon_reviews_us_Video_Games_v1_00.tsv"

df_imdb = spark.read.option("header", True).csv(imdb_path)
print("IMDb Dataset:")
df_imdb.show(5)

df_amazon = spark.read.option("header", True).option("sep", "\t").csv(amazon_path)
print("Amazon Dataset:")
df_amazon.show(5)

df_imdb.printSchema()
df_amazon.printSchema()

spark.stop()

