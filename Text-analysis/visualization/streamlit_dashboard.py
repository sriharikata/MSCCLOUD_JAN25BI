import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from kafka import KafkaConsumer
import json
import boto3
import io
import time
from datetime import datetime

# Configurations
KAFKA_TOPIC = "reviews"
#KAFKA_BOOTSTRAP_SERVERS = "52.91.15.28:9092"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
S3_BUCKET = "txt-analysis-results"
S3_PREFIX = "benchmark/"
STOP_WORDS = set(["the", "is", "in", "and", "a", "to", "of"])

# Initialize S3 client
s3_client = boto3.client('s3')

# Helper: Load files from S3
def load_s3_file(filename, file_type="csv"):
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}{filename}")
        if file_type == "csv":
            return pd.read_csv(io.BytesIO(obj['Body'].read()))
        elif file_type == "json":
            return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"Error loading {filename}: {e}")
        return None

# Streamlit page config
st.set_page_config(page_title="Scalable Cloud Programming Dashboard", layout="wide")
st.title("📊 Scalable Cloud Programming Dashboard")

# Sidebar controls
st.sidebar.title("Controls")
if st.sidebar.button("▶️ Start Producer"):
    import subprocess
    subprocess.Popen(["python3", "ingestion/kafka_s3_producer.py"])
    st.sidebar.success("✅ Producer started.")

if st.sidebar.button("⚡ Run Benchmark"):
    import subprocess
    subprocess.run(["python3", "batch/Performance_hybrid_wordcount.py"])
    st.sidebar.success("✅ Benchmark completed and uploaded to S3.")

# Live Analytics without threading
if st.sidebar.button("▶️ Refresh Live Analytics"):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=None,
        consumer_timeout_ms=5000
    )

    word_counter = Counter()
    sentiment_counter = Counter()
    total_count = 0

    for message in consumer:
        data = message.value
        review = data.get('review', '')
        sentiment = data.get('sentiment', '').lower()
        words = [w.lower() for w in review.split() if w.isalpha() and w.lower() not in STOP_WORDS]

        word_counter.update(words)
        sentiment_counter.update([sentiment])
        total_count += 1

    st.subheader("🔴 Live Streaming Analytics")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Words")
        top_words_df = pd.DataFrame(word_counter.most_common(10), columns=["Word", "Count"]).set_index("Word")
        st.bar_chart(top_words_df)

    with col2:
        st.subheader("Sentiment Distribution")
        if sentiment_counter:
            # Escape problematic characters in labels
            labels = [label.replace('$', '\\$') for label in st.session_state['sentiment_counter'].keys()]
            sizes = list(st.session_state['sentiment_counter'].values())

            fig, ax = plt.subplots()
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
            ax.axis("equal")
            st.pyplot(fig)
            plt.close(fig)

    st.metric("Total Records Processed", str(total_count))

# Benchmark Results Section
st.markdown("---")
st.header("📈 Batch Benchmark Results")

available_files = []
try:
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    if 'Contents' in response:
        available_files = [
            obj['Key'].split('/')[-1]
            for obj in response['Contents']
            if obj['Key'].endswith('.csv')
        ]
except Exception as e:
    st.error(f"Error listing S3 files: {e}")

selected_file = st.sidebar.selectbox(
    "Select Benchmark Result CSV",
    available_files,
    index=0 if available_files else None
)

if selected_file:
    df = load_s3_file(selected_file, "csv")
    if df is not None:
        st.subheader(f"Benchmark Results: {selected_file}")
        st.dataframe(df, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Throughput vs Dataset Size")
            fig, ax = plt.subplots()
            for w in sorted(df['Workers'].unique()):
                subset = df[df['Workers'] == w]
                ax.plot(subset['Size'], subset['Throughput(rps)'], marker='o', label=f"{w} workers")
            ax.set_xlabel("Dataset Size")
            ax.set_ylabel("Throughput (records/sec)")
            ax.legend()
            st.pyplot(fig)
            plt.close(fig)
        with col2:
            st.subheader("Latency vs Dataset Size for WordCount Benchmark")
        
            fig, ax = plt.subplots(figsize=(8, 5))
        
            for w in sorted(df['Workers'].unique()):
                subset = df[df['Workers'] == w]
                ax.plot(
                    subset['Size'],
                    subset['Latency(s/record)'],
                    marker='o',
                    label=f"{w} workers"
                )
        
            ax.set_xlabel("Dataset Size (records)")
            ax.set_ylabel("Latency (seconds/record)")
            ax.set_title("Latency vs Dataset Size (Log Scale)")
        
            # Use log scale to show small differences clearly
            ax.set_yscale('log')
        
            # Add grid for readability
            ax.grid(True, linestyle='--', alpha=0.5)
        
            # Position the legend clearly
            ax.legend(title="Workers", loc='upper left')
        
            # Render the plot in Streamlit
            st.pyplot(fig)
            plt.close(fig)
       
else:
    st.info("Upload benchmark CSV files to your S3 bucket for visualization.")

st.markdown("---")
st.info("✅ This final clean dashboard enables safe, thread-free live streaming analytics and batch benchmarking visualization for your MSc submission.")

