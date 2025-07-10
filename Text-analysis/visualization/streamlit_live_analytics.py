import streamlit as st
import pandas as pd
import boto3
import io
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from datetime import datetime

# ---------------- Configurations ----------------
S3_BUCKET = "txt-analysis-results"
S3_PREFIX = "wordcount-stream/"
s3_client = boto3.client('s3')

# ---------------- Streamlit Page Setup ----------------
st.set_page_config(page_title="Word Count Streaming Dashboard", layout="wide")
st.title("📊 Real-Time Word Count Streaming Dashboard")

# Sidebar controls
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 30, 10)
top_n = st.sidebar.slider("Top N Words", 5, 20, 10)

# ---------------- Fetch Latest Parquet Files ----------------
def fetch_latest_parquet():
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    if 'Contents' not in response:
        st.warning("No streaming data available yet.")
        return pd.DataFrame()
    # Get the latest files (last 5 for aggregation)
    files = sorted([obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')], reverse=True)[:5]
    dfs = []
    for file in files:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file)
        data = obj['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        dfs.append(df)
    if dfs:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()

# ---------------- Main Dashboard ----------------
data_load_state = st.text("Loading data...")
df = fetch_latest_parquet()
data_load_state.text("Data loaded.")

if not df.empty:
    # Convert window start to datetime
    df['window_start'] = pd.to_datetime(df['window'].apply(lambda x: x['start']))

    # Aggregate for Top N words
    top_words = df.groupby('word')['count'].sum().sort_values(ascending=False).head(top_n)
    st.subheader(f"Top {top_n} Words in Recent Windows")
    st.bar_chart(top_words)

    # Line chart of word trends
    st.subheader(f"Word Trends Over Time (Top {top_n} Words)")
    top_words_list = top_words.index.tolist()
    df_filtered = df[df['word'].isin(top_words_list)]
    trend_df = df_filtered.groupby(['window_start', 'word'])['count'].sum().reset_index()
    trend_pivot = trend_df.pivot(index='window_start', columns='word', values='count').fillna(0)
    st.line_chart(trend_pivot)

    # Optional raw data table
    with st.expander("View Raw Windowed Data"):
        st.dataframe(df)
else:
    st.info("Waiting for streaming word count data...")

# Auto-refresh mechanism
st.rerun()

