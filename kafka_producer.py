from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("IMDB Dataset.csv", "r") as f:
    next(f)  # skip header
    for line in f:
        review, sentiment = line.strip().split(",", 1)
        message = {"review": review, "sentiment": sentiment}
        producer.send("reviews", value=message)
        print("Sent:", message)
        time.sleep(0.5)
