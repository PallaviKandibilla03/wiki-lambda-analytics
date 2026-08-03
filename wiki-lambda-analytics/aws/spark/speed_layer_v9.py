#!/usr/bin/env python3
"""
Wiki Lambda Analytics - Speed Layer V9

Reads Wikimedia edit events from Kinesis, maintains a sliding window
(default 5 minutes), compares each article's current edit rate against
its historical baseline (computed by the batch layer), and writes a
combined Lambda-architecture snapshot to S3 every batch interval.

Run on the EMR master node, e.g.:

    spark-submit speed_layer_v9.py \
        --stream-name wiki-lambda-stream \
        --region us-east-1 \
        --output-s3 s3://wiki-lambda-x23352370/speed_layer_output_v9 \
        --baseline-s3 s3://wiki-lambda-x23352370/processed/article_baseline/ \
        --window-seconds 300 \
        --batch-seconds 5 \
        --top-n 10
"""

from __future__ import annotations

import argparse
import json
import time
from collections import deque, defaultdict
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--output-s3", required=True,
                         help="e.g. s3://bucket/speed_layer_output_v9 (no trailing slash)")
    parser.add_argument("--baseline-s3", required=True,
                         help="e.g. s3://bucket/processed/article_baseline/")
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--batch-seconds", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=10)

    return parser.parse_args()


# =========================================================
# S3 HELPERS
# =========================================================

def parse_bucket_prefix(s3_uri: str) -> tuple[str, str]:
    assert s3_uri.startswith("s3://"), f"Invalid S3 URI: {s3_uri}"
    rest = s3_uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix.rstrip("/")


def s3_write_json(s3_client, bucket: str, key: str, payload: dict) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


# =========================================================
# BATCH BASELINE LOADER
# =========================================================

def load_baseline(spark: SparkSession, baseline_s3: str) -> dict[str, float]:
    """
    Load the historical article_baseline parquet produced by the batch
    layer into an in-memory lookup: title -> baseline_edits_per_hour.
    """
    df = spark.read.parquet(baseline_s3)
    rows = df.select("title", "baseline_edits_per_hour").collect()

    baseline = {}
    for row in rows:
        title = row["title"]
        value = row["baseline_edits_per_hour"]
        if title is not None and value is not None:
            baseline[title] = float(value)

    return baseline


# =========================================================
# KINESIS CONSUMER
# =========================================================

def get_all_shard_iterators(kinesis_client, stream_name: str) -> dict[str, str]:
    desc = kinesis_client.describe_stream(StreamName=stream_name)
    shard_ids = [s["ShardId"] for s in desc["StreamDescription"]["Shards"]]

    iterators = {}
    for shard_id in shard_ids:
        resp = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="LATEST",
        )
        iterators[shard_id] = resp["ShardIterator"]

    return iterators


def extract_title(record_data: bytes) -> str | None:
    try:
        payload = json.loads(record_data)
    except Exception:
        return None

    # Wikimedia EventStreams recent-change payloads use "title";
    # tolerate a couple of alternate keys just in case.
    return payload.get("title") or payload.get("page_title")


# =========================================================
# MAIN LOOP
# =========================================================

def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("WikiLambdaSpeedLayerV9").getOrCreate()

    print(
        f"[CONFIG] stream={args.stream_name} region={args.region} "
        f"output={args.output_s3} baseline={args.baseline_s3} "
        f"window={args.window_seconds}s batch={args.batch_seconds}s top_n={args.top_n}"
    )

    baseline = load_baseline(spark, args.baseline_s3)
    print(f"[INFO] Loaded {len(baseline)} historical article baselines")

    kinesis = boto3.client("kinesis", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    out_bucket, out_prefix = parse_bucket_prefix(args.output_s3)

    shard_iterators = get_all_shard_iterators(kinesis, args.stream_name)
    print(f"[INFO] Tracking {len(shard_iterators)} shard(s)")

    # Sliding window of (event_epoch_seconds, title)
    window: deque[tuple[float, str]] = deque()

    def prune_window(now_epoch: float) -> None:
        cutoff = now_epoch - args.window_seconds
        while window and window[0][0] < cutoff:
            window.popleft()

    print("[INFO] Speed Layer V9 running. Writing snapshots to "
          f"s3://{out_bucket}/{out_prefix}/latest.json")

    while True:
        loop_start = time.time()

        for shard_id, iterator in list(shard_iterators.items()):
            if iterator is None:
                # Iterator expired or shard closed; try to re-acquire.
                try:
                    resp = kinesis.get_shard_iterator(
                        StreamName=args.stream_name,
                        ShardId=shard_id,
                        ShardIteratorType="LATEST",
                    )
                    shard_iterators[shard_id] = resp["ShardIterator"]
                except Exception as exc:
                    print(f"[WARN] could not refresh iterator for {shard_id}: {exc}")
                continue

            try:
                resp = kinesis.get_records(ShardIterator=iterator, Limit=1000)
            except Exception as exc:
                print(f"[WARN] get_records failed for {shard_id}: {exc}")
                shard_iterators[shard_id] = None
                continue

            shard_iterators[shard_id] = resp.get("NextShardIterator")

            now = time.time()
            for record in resp.get("Records", []):
                title = extract_title(record["Data"])
                if title:
                    window.append((now, title))

        now_epoch = time.time()
        prune_window(now_epoch)

        # ---- aggregate counts per article in the current window ----
        counts: dict[str, int] = defaultdict(int)
        for _, title in window:
            counts[title] += 1

        articles = []
        for title, recent_edits in counts.items():
            current_rate = recent_edits * (3600.0 / args.window_seconds)
            base = baseline.get(title)

            if base and base > 0:
                trend = current_rate / base
                articles.append({
                    "title": title,
                    "recent_edits": recent_edits,
                    "current_edits_per_hour": round(current_rate, 2),
                    "baseline_edits_per_hour": round(base, 2),
                    "trend_score": round(trend, 2),
                    "baseline_available": True,
                })
            else:
                articles.append({
                    "title": title,
                    "recent_edits": recent_edits,
                    "current_edits_per_hour": round(current_rate, 2),
                    "baseline_edits_per_hour": None,
                    "trend_score": None,
                    "baseline_available": False,
                })

        # baseline_available first, then trend_score, then raw rate
        articles.sort(
            key=lambda a: (
                a["baseline_available"],
                a["trend_score"] if a["trend_score"] is not None else -1.0,
                a["current_edits_per_hour"],
            ),
            reverse=True,
        )

        top_articles = articles[: args.top_n]

        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_seconds": args.window_seconds,
            "total_events_in_window": len(window),
            "distinct_articles_in_window": len(counts),
            "trending_articles": top_articles,
        }

        try:
            s3_write_json(s3, out_bucket, f"{out_prefix}/latest.json", snapshot)

            ts = int(now_epoch)
            s3_write_json(s3, out_bucket, f"{out_prefix}/history/{ts}.json", snapshot)
        except Exception as exc:
            print(f"[WARN] failed to write snapshot to S3: {exc}")

        elapsed = time.time() - loop_start
        sleep_for = max(0.0, args.batch_seconds - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
