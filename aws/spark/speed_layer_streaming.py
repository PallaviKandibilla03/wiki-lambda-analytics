"""
Speed Layer - Spark Streaming job (runs on EMR).
Reads records from Kinesis, applies sliding-window aggregation, flags
trending articles, writes snapshots to S3, publishes CloudWatch metrics.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

import boto3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import InitialPositionInStream, KinesisUtils

TREND_MIN_EDITS_FALLBACK = 3
TOP_N = 10


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--stream-name", required=True)
    p.add_argument("--app-name", default="WikiSpeedLayerApp")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--output-s3", required=True)
    p.add_argument("--window-seconds", type=int, default=300)
    p.add_argument("--slide-seconds", type=int, default=10)
    p.add_argument("--batch-seconds", type=int, default=5)
    p.add_argument("--cloudwatch-namespace", default="WikiLambdaAnalytics")
    return p.parse_args()


def parse_record(raw: bytes):
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        obj = json.loads(text)
        title = obj.get("title")
        if not title:
            return None
        return (title, 1)
    except Exception:
        return None


def make_batch_processor(output_s3: str, region: str, cw_namespace: str):
    def process(time_, rdd) -> None:
        if rdd.isEmpty():
            return
        counts = rdd.collect()
        total_events = sum(c for _, c in counts)
        trending = sorted(
            [
                {"title": title, "recent_edits": count, "trend_score": float(count)}
                for title, count in counts
                if count > TREND_MIN_EDITS_FALLBACK
            ],
            key=lambda x: x["trend_score"],
            reverse=True,
        )[:TOP_N]

        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_events_in_window": total_events,
            "distinct_articles_in_window": len(counts),
            "trending_articles": trending,
        }

        s3 = boto3.client("s3", region_name=region)
        bucket, _, prefix = output_s3.replace("s3://", "").partition("/")
        body = json.dumps(snapshot).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/latest.json", Body=body)
        s3.put_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/history/{int(time.time())}.json", Body=body)

        cloudwatch = boto3.client("cloudwatch", region_name=region)
        cloudwatch.put_metric_data(
            Namespace=cw_namespace,
            MetricData=[
                {"MetricName": "SpeedLayerWindowEvents", "Value": float(total_events), "Unit": "Count"},
                {"MetricName": "SpeedLayerTrendingArticleCount", "Value": float(len(trending)), "Unit": "Count"},
            ],
        )
    return process


def main() -> None:
    args = parse_args()
    sc = SparkContext(appName=args.app_name)
    ssc = StreamingContext(sc, args.batch_seconds)
    ssc.checkpoint(f"{args.output_s3.rstrip('/')}/_checkpoint")

    stream = KinesisUtils.createStream(
        ssc, args.app_name, args.stream_name,
        f"https://kinesis.{args.region}.amazonaws.com", args.region,
        InitialPositionInStream.LATEST, args.batch_seconds,
    )

    parsed = stream.map(parse_record).filter(lambda x: x is not None)
    windowed = parsed.reduceByKeyAndWindow(
        lambda a, b: a + b, lambda a, b: a - b,
        windowDuration=args.window_seconds, slideDuration=args.slide_seconds,
    )
    windowed.foreachRDD(make_batch_processor(args.output_s3, args.region, args.cloudwatch_namespace))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()