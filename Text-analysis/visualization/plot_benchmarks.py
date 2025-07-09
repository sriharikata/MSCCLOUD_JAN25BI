import matplotlib.pyplot as plt


results = [
    # (size, workers, time, throughput, latency)
    (1000, 1, 0.25, 4000, 0.00025),
    (1000, 2, 0.17, 5882, 0.00017),
    (1000, 4, 0.12, 8333, 0.00012),
    (1000, 8, 0.10, 10000, 0.00010),
    (5000, 1, 1.5, 3333, 0.0003),
    (5000, 4, 0.8, 6250, 0.00016),
]

# Extracting unique sizes and worker counts
sizes = sorted(set(r[0] for r in results))
worker_set = sorted(set(r[1] for r in results))

# Plot throughput
plt.figure(figsize=(8, 5))
for w in worker_set:
    data = [r for r in results if r[1] == w]
    x = [r[0] for r in data]
    y = [r[3] for r in data]
    plt.plot(x, y, marker='o', label=f'{w} workers')
plt.title("Throughput vs Dataset Size")
plt.xlabel("Dataset Size")
plt.ylabel("Throughput (records/sec)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("throughput_plot.png")
plt.show()

# Plot latency
plt.figure(figsize=(8, 5))
for w in worker_set:
    data = [r for r in results if r[1] == w]
    x = [r[0] for r in data]
    y = [r[4] for r in data]
    plt.plot(x, y, marker='o', label=f'{w} workers')
plt.title("Latency vs Dataset Size")
plt.xlabel("Dataset Size")
plt.ylabel("Latency (seconds/record)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("latency_plot.png")
plt.show()
