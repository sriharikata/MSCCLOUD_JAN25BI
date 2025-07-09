import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import json
from collections import Counter
from nltk.corpus import stopwords
import nltk
import time

nltk.download('stopwords')
STOP_WORDS = set(stopwords.words('english'))

KAFKA_TOPIC = 'reviews'
KAFKA_BOOTSTRAP_SERVERS = '35.170.203.165:9092'

# Initialize session state for analytics
if 'run_analytics' not in st.session_state:
    st.session_state['run_analytics'] = False

st.title("📊 MSc Scalable Cloud Programming Dashboard")
st.markdown("""
This dashboard demonstrates **real-time streaming ingestion, live processing, and analytics** for your MSc project:
- ✅ Ingest IMDB data from **S3 → Kafka**.
- ✅ Display **live Top 10 Word Counts**.
- ✅ Display **live Sentiment Analysis**.
- ✅ Run **Batch Benchmarking**.
""")

# ---- Buttons ----
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("▶️ Start Producer"):
        import subprocess
        subprocess.Popen(['python3', 'ingestion/kafka_s3_producer.py'])
        st.success("✅ Producer started.")
with col2:
    if st.button("▶️ Start Live Analytics"):
        st.session_state['run_analytics'] = True
with col3:
    if st.button("⚡ Run Benchmark"):
        import subprocess
        subprocess.run(['python3', 'batch/Performance_hybrid_wordcount.py'])
        st.success("✅ Benchmark completed.")

st.markdown("---")

# ---- Live Analytics ----
if st.session_state['run_analytics']:
    st.subheader("🔴 Live Stream Analytics")

    word_placeholder = st.empty()
    sentiment_placeholder = st.empty()
    count_placeholder = st.empty()

    word_counter = Counter()
    sentiment_counter = Counter()
    total_count = 0

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None
    )

    # Poll messages and update charts
    while True:
        raw_msgs = consumer.poll(timeout_ms=1000)
        updated = False

        for tp, messages in raw_msgs.items():
            for message in messages:
                data = message.value
                review = data.get('review', '')
                sentiment = data.get('sentiment', '').lower()

                words = [w.lower() for w in review.split() if w.isalpha() and w.lower() not in STOP_WORDS]
                word_counter.update(words)
                sentiment_counter.update([sentiment])
                total_count += 1
                updated = True

        if updated:
            top_words_df = pd.DataFrame(word_counter.most_common(10), columns=['Word', 'Count']).set_index('Word')
            word_placeholder.bar_chart(top_words_df)

            if sentiment_counter:
                labels = list(sentiment_counter.keys())
                sizes = list(sentiment_counter.values())
                fig, ax = plt.subplots()
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
                ax.axis('equal')
                sentiment_placeholder.pyplot(fig)
                plt.close(fig)

            count_placeholder.metric(label="Total Records Processed", value=str(total_count))

        time.sleep(2)  # update every 2 seconds
        st.rerun()

# ---- Benchmark Display ----
st.markdown("---")
st.header("📈 Batch Benchmark Results")
try:
    df = pd.read_csv("batch/benchmark_results.csv")
    st.dataframe(df)

    fig1, ax1 = plt.subplots()
    for w in sorted(df['Workers'].unique()):
        subset = df[df['Workers'] == w]
        ax1.plot(subset['Size'], subset['Throughput(rps)'], marker='o', label=f"{w} workers")
    ax1.set_title("Throughput vs Dataset Size")
    ax1.set_xlabel("Dataset Size")
    ax1.set_ylabel("Throughput (records/sec)")
    ax1.legend()
    st.pyplot(fig1)
    plt.close(fig1)

    fig2, ax2 = plt.subplots()
    for w in sorted(df['Workers'].unique()):
        subset = df[df['Workers'] == w]
        ax2.plot(subset['Size'], subset['Latency(s/record)'], marker='o', label=f"{w} workers")
    ax2.set_title("Latency vs Dataset Size")
    ax2.set_xlabel("Dataset Size")
    ax2.set_ylabel("Latency (s/record)")
    ax2.legend()
    st.pyplot(fig2)
    plt.close(fig2)

except FileNotFoundError:
    st.info("⚠️ Run the benchmark to view charts.")

st.markdown("---")
st.info("✅ This dashboard is now fully ready to demonstrate your MSc submission with clean, live analytics and benchmarking.")

