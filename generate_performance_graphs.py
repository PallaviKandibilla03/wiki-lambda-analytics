import matplotlib.pyplot as plt


# ============================================================
# GRAPH 1 — SPEEDUP VS EXECUTOR COUNT
# ============================================================

executors = [1, 2]
runtimes = [80.055, 78.075]

baseline_runtime = runtimes[0]
speedup = [
    baseline_runtime / runtime
    for runtime in runtimes
]

plt.figure(figsize=(7, 4.5))
plt.plot(executors, speedup, marker="o", linewidth=2)

for x, y in zip(executors, speedup):
    plt.text(
        x,
        y + 0.002,
        f"{y:.3f}x",
        ha="center"
    )

plt.xlabel("Number of Spark Executors")
plt.ylabel("Speedup")
plt.title("Batch Processing Speedup vs Executor Count")
plt.xticks(executors)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    "speedup_vs_executors.png",
    dpi=300
)
plt.close()


# ============================================================
# GRAPH 2 — PROCESSING LATENCY VS WINDOW LOAD
# ============================================================

window_events = [
    27.4,
    81.0,
    132.25
]

average_latency = [
    487.504,
    295.470,
    282.628
]

plt.figure(figsize=(7, 4.5))
plt.plot(
    window_events,
    average_latency,
    marker="o",
    linewidth=2
)

for x, y in zip(
    window_events,
    average_latency
):
    plt.text(
        x,
        y + 8,
        f"{y:.1f} ms",
        ha="center"
    )

plt.xlabel("Average Events in 5-Minute Sliding Window")
plt.ylabel("Processing Latency (ms)")
plt.title(
    "Speed-Layer Processing Latency vs Window Load"
)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    "latency_vs_window_load.png",
    dpi=300
)
plt.close()


# ============================================================
# GRAPH 3 — INGESTION THROUGHPUT OVER TIME
# ============================================================

times = [
    "22:01", "22:06", "22:11", "22:16",
    "22:21", "22:26", "22:31", "22:36",
    "22:41", "22:46", "22:51", "22:56",
    "23:01", "23:06", "23:11", "23:16",
    "23:21", "23:26", "23:31", "23:36",
    "23:41", "23:46", "23:51", "23:56"
]

average_eps = [
    1.9039,
    1.3761,
    1.3810,
    1.4839,
    1.3702,
    1.2796,
    1.3165,
    1.8280,
    1.4337,
    1.5459,
    1.6793,
    1.6074,
    1.3542,
    1.4658,
    1.5208,
    1.4712,
    1.3789,
    1.3897,
    1.3578,
    1.2021,
    1.2556,
    1.1277,
    1.1315,
    1.0489
]

maximum_eps = [
    5.2018,
    2.2054,
    1.9865,
    1.9211,
    2.0980,
    1.9682,
    2.0153,
    5.4861,
    2.5728,
    2.5933,
    2.4142,
    4.9487,
    2.1486,
    2.2658,
    2.3382,
    2.1883,
    2.1285,
    2.0607,
    2.1786,
    2.0991,
    2.1915,
    1.7822,
    2.1572,
    1.6964
]

x = range(len(times))

plt.figure(figsize=(10, 5))

plt.plot(
    x,
    average_eps,
    marker="o",
    label="Average Events/sec"
)

plt.plot(
    x,
    maximum_eps,
    linestyle="--",
    label="Maximum Events/sec"
)

plt.xlabel("Time")
plt.ylabel("Events per Second")
plt.title("Wikimedia Ingestion Throughput Over Time")

plt.xticks(
    list(x)[::2],
    times[::2],
    rotation=45
)

plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()

plt.savefig(
    "throughput_over_time.png",
    dpi=300
)

plt.close()


print("======================================")
print(" Performance graphs generated")
print("======================================")
print("1. speedup_vs_executors.png")
print("2. latency_vs_window_load.png")
print("3. throughput_over_time.png")