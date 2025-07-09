import time
import multiprocessing as mp
from collections import Counter

# Loading dataset
def load_reviews(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

# Tokenizer
def tokenize(text):
    return [word.lower() for word in text.split() if word.isalpha()]

# Sequential processing
def wordcount_sequential(data):
    words = []
    for review in data:
        words.extend(tokenize(review))
    return Counter(words)

# Parallel map function
def count_words_chunk(chunk):
    words = []
    for review in chunk:
        words.extend(tokenize(review))
    return Counter(words)

# Parallel processing
def wordcount_parallel(data, num_workers=4):
    chunk_size = len(data) // num_workers
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    with mp.Pool(processes=num_workers) as pool:
        results = pool.map(count_words_chunk, chunks)

    final_counts = Counter()
    for c in results:
        final_counts.update(c)
    return final_counts

# Benchmarking
def benchmark(data):
    print("\n Running Sequential Word Count...")
    t1 = time.time()
    seq_result = wordcount_sequential(data)
    t2 = time.time()
    print(f" Sequential done in {t2 - t1:.2f} seconds")

    print("\n Running Parallel Word Count with 4 workers...")
    t3 = time.time()
    par_result = wordcount_parallel(data, num_workers=4)
    t4 = time.time()
    print(f" Parallel done in {t4 - t3:.2f} seconds")

    print("\n Top 10 words (parallel):")
    for word, count in par_result.most_common(10):
        print(f"{word}: {count}")

# MAIN
if __name__ == "__main__":
    dataset_path = "/home/ubuntu/Text-analysis/datasets/IMDB_Dataset_Expanded.csv"
    #dataset_path = "IMDB Dataset.csv"

    print(" Loading dataset...")
    raw_data = load_reviews(dataset_path)

    # Extracting only  review texts
    if "," in raw_data[0]:
        data = [line.split(",")[0] for line in raw_data]
    else:
        data = raw_data

    benchmark(data)
