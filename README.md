#  Scalable Real-Time Text Analysis Using Python, Spark, Kafka, and AWS

This project demonstrates a scalable, cloud-based system to ingest, process, and analyze large volumes of text data using distributed computing and stream processing. The implementation leverages Apache Kafka, Apache Spark, AWS S3, and Python-based multiprocessing to support both batch and real-time processing.

---

##  Project Objectives

- Ingest text data from AWS S3 using Spark
- Send the data to a Kafka topic simulating real-time streaming
- Consume and process Kafka data using Spark Structured Streaming
- Perform parallel batch processing using Python's multiprocessing
- Measure performance (throughput, latency) and visualize results

---

##  Repository Contents

| File                              | Description |
|-----------------------------------|-------------|
| `kafka_s3_producer.py`            | Reads text data from AWS S3 and streams it into a Kafka topic (`reviews`). |
| `spark_kafka_consumer.py`         | Spark Structured Streaming consumer that processes Kafka messages in real-time. |
| `spark_kafka_streaming_window.py` | Streaming consumer that uses windowed aggregation to compute top trending words in 5-minute sliding windows. |
| `kafka_producer.py`               | Kafka message producer (local test version, not S3-based). |
| `producer.py` / `producer.log`    | Supporting test producer and its logs. |
| `load_datasets.py`                | Helper to load and prepare dataset from disk or S3. |
| `mapreduce_wordcount.py`          | Implements MapReduce-based word count using Python `multiprocessing`. |
| `hybrid_wordcount.py`             | Benchmarking script comparing sequential vs. parallel word count under different loads. |
| `Performance_hybrid_wordcount.py` | Benchmarking results analyzer (time, throughput, latency). |
| `plot_benchmarks.py`              | Generates performance plots from benchmarking results. |
| `throughput_plot.png`             | Graph showing throughput vs. dataset size and workers. |
| `latency_plot.png`                | Graph showing latency per record across dataset sizes. |

---

##  System Architecture
![image](https://github.com/user-attachments/assets/87a5857b-d9e5-4727-9b27-3b8f8d1edf95)


##  How to Run

###  1. Run Kafka Producer

```bash
source py310-env/bin/activate
env JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 PATH=$JAVA_HOME/bin:$PATH \
nohup python3 kafka_s3_producer.py > producer.log 2>&1 &
```
###  2. Start Spark Kafka Consumer
`$SPARK_HOME/bin/spark-submit spark_kafka_consumer.py`

### 3. Run Parallel Word Count Benchmark
`python3 hybrid_wordcount.py`

#  Results
![latency_plot](https://github.com/user-attachments/assets/aeda9416-2734-423b-89db-be872f2bb948)

![throughput_plot](https://github.com/user-attachments/assets/f5fed219-ab54-4220-833b-0047914bc474)

#  Tools & Technologies
 - Python 3.10
 - Apache Kafka 3.5.1
 - Apache Spark 3.5.6
 - AWS S3
 - PySpark
 - Matplotlib
 - Multiprocessing (Python)
