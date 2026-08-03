import matplotlib.pyplot as plt

ingestion_eps = [
    0.9528842509,
    0.9297403528,
    0.8594842856,
    1.1174982547,
    1.0301055045,
    0.9984227553,
    0.8088021905,
    0.8078900021
]

latency_ms = [
    487.5041757,
    295.4702214,
    282.6277386,
    274.4819048,
    260.4861807,
    258.9051954,
    254.9837430,
    254.1284360
]

plt.figure(figsize=(7, 4.5))

plt.scatter(
    ingestion_eps,
    latency_ms,
    s=70
)

plt.xlabel("Ingestion Rate (Events/Second)")
plt.ylabel("Speed-Layer Processing Latency (ms)")
plt.title("Processing Latency vs Ingestion Rate")

plt.grid(True, alpha=0.3)
plt.tight_layout()

plt.savefig(
    "latency_vs_ingestion_rate.png",
    dpi=300
)

plt.close()

print("Generated: latency_vs_ingestion_rate.png")