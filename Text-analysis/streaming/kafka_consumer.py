from kafka import KafkaConsumer
from collections import Counter
import json
import time

KAFKA_TOPIC = "reviews"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
STOP_WORDS = set(["the", "is", "in", "and", "a", "to", "of"])

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="test-consumer"
)

word_counter = Counter()
sentiment_counter = Counter()
total_count = 0

print("✅ Kafka consumer started. Waiting for messages...\n")

try:
    for message in consumer:
        data = message.value
        review = data.get('review', '')
        sentiment = data.get('sentiment', '').lower()
        words = [w.lower() for w in review.split() if w.isalpha() and w.lower() not in STOP_WORDS]
        word_counter.update(words)
        sentiment_counter.update([sentiment])
        total_count += 1

        # Print last message
        print(f"\n--- Message {total_count} ---")
        print(f"Review: {review[:80]}...")  # print only first 80 chars
        print(f"Sentiment: {sentiment}")

        # Print top 10 words
        print("\nTop 10 Words:")
        for word, count in word_counter.most_common(10):
            print(f"{word}: {count}")

        # Print sentiment distribution
        print("\nSentiment Distribution:")
        for sent, count in sentiment_counter.items():
            print(f"{sent}: {count}")

        time.sleep(0.5)  # slow down for readability

except KeyboardInterrupt:
    print("\n✅ Stopped by user.")
finally:
    consumer.close()

