import boto3
import json
import time

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')  # Change region if needed

stream_name = 'text-stream'

with open('IMDB Dataset.csv', 'r') as file:
    next(file)  # skip header
    for line in file:
        review, sentiment = line.strip().split(',', 1)
        payload = json.dumps({"review": review, "sentiment": sentiment})
        kinesis.put_record(
            StreamName=stream_name,
            Data=payload.encode('utf-8'),
            PartitionKey="partitionKey"
        )
        print(f"Sent: {payload}")
        time.sleep(0.5)  # Simulate streaming
