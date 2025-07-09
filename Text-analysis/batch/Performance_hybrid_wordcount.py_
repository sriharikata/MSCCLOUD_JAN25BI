import time
import csv
import multiprocessing as mp
from collections import Counter

# Load reviews from a CSV file
def load_reviews(path):
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    return [line.strip().split(',')[0] for line in lines if line.strip()]

# Tokenize words
def tokenize(text):
    return [word.lower() for word in text.split() if word.isalpha()]

# Count words sequentially
def wordcount_sequential(data):
    words = []
    for review in data:
        words.extend(tokenize(review))
    return Counter(words)

# Count words in a chunk
def count_words_chunk(chunk):
    words = []
    for review in chunk:
        words.extend(tokenize(review))
    return Counter(words)

# Count words in parallel
def wordcount_parallel(data, num_workers=4):
    chunk_size = len(data) // num_workers
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    with mp.Pool(processes=num_workers) as pool:
        results = pool.map(count_words_chunk, chunks)

    final_counts = Counter()
    for c in results:
        final_counts.update(c)
    return final_counts

# Benchmark function
def benchmark(data):
    sizes = [1000, 5000, 10000, 50000, 100000, 150000, len(data)]
    #sizes = [1000, 5000, 10000, 20000, len(data)]
    workers = [1, 2, 4, 8]
    
    results = []

    print("Size\tWorkers\tTime(s)\tThroughput(rps)\tLatency(s/record)")
    for size in sizes:
        chunk = data[:size]
        for w in workers:
            start = time.time()
            wordcount_parallel(chunk, num_workers=w)
            end = time.time()
            duration = end - start
            throughput = size / duration
            latency = duration / size
            print(f"{size}\t{w}\t{round(duration,2)}\t{round(throughput,2)}\t\t{round(latency,4)}")
            results.append((size, w, round(duration, 2), round(throughput, 2), round(latency, 4)))
   # Save to CSV for Streamlit dashboard
    with open('benchmark_results.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Size', 'Workers', 'Time(s)', 'Throughput(rps)', 'Latency(s/record)'])
        writer.writerows(results)
    return results

# MAIN
if __name__ == "__main__":
    print(" Loading dataset...")
    dataset_path = "/home/ubuntu/Text-analysis/datasets/IMDB_Dataset_Expanded.csv"
    #dataset_path = "IMDB Dataset.csv"
    data = load_reviews(dataset_path)
    benchmark(data)
