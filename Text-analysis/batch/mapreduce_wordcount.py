import multiprocessing
from collections import Counter
import csv
import re

# Cleaning and tokenizing review text
def tokenize(text):
    text = text.lower()
    words = re.findall(r'\b[a-z]+\b', text)
    return words

# Map step: counting words in a chunk
def map_reviews(chunk):
    word_counts = Counter()
    for review in chunk:
        words = tokenize(review)
        word_counts.update(words)
    return word_counts

# Reduce step: mergeing results from all mappers
def reduce_counts(mapped_results):
    final_counts = Counter()
    for result in mapped_results:
        final_counts.update(result)
    return final_counts

# Loading data from CSV
def load_reviews(path):
    reviews = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'review' in row and row['review']:
                reviews.append(row['review'])
    return reviews

if __name__ == "__main__":
    data_path = "/home/ubuntu/Text-analysis/datasets/IMDB Dataset.csv"
    reviews = load_reviews(data_path)

    # Splitting data into chunks for each process
    num_workers = multiprocessing.cpu_count()
    chunk_size = len(reviews) // num_workers
    chunks = [reviews[i:i + chunk_size] for i in range(0, len(reviews), chunk_size)]

    print(f"Running MapReduce with {num_workers} parallel workers...")

    with multiprocessing.Pool(num_workers) as pool:
        mapped = pool.map(map_reviews, chunks)
        result = reduce_counts(mapped)

    # Showing top 10 most common words
    print("Top 10 words:")
    for word, count in result.most_common(10):
        print(f"{word}: {count}")
