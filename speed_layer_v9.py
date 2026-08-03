"""
Wiki Lambda Analytics - Speed Layer V9

Architecture:
Kinesis
   -> boto3 polling
   -> Spark aggregation
   -> sliding in-memory window
   -> Batch-layer historical baseline
   -> Lambda combined trend score
   -> S3 latest/history snapshots
   -> CloudWatch metrics

Trend calculation:

    current_edits_per_hour =
        recent_edits * (3600 / window_seconds)

    trend_score =
        current_edits_per_hour / baseline_edits_per_hour

The historical baseline is loaded once from the Batch Layer when the
application starts.

This implementation intentionally avoids Spark KCL/KinesisUtils because
the restricted EMR lab role does not provide all KCL permissions.
"""

from __future__ import annotations

import argparse
import json
import time
import traceback
from collections import deque
from datetime import datetime, timezone

import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession


# =========================================================
# CONFIGURATION
# =========================================================

TREND_MIN_EDITS = 3
TOP_N = 10


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--stream-name",
        required=True,
    )

    parser.add_argument(
        "--region",
        default="us-east-1",
    )

    parser.add_argument(
        "--output-s3",
        required=True,
    )

    parser.add_argument(
        "--baseline-s3",
        default=None,
        help=(
            "S3 path containing Batch Layer "
            "article baseline Parquet files"
        ),
    )

    parser.add_argument(
        "--window-seconds",
        type=int,
        default=300,
    )

    parser.add_argument(
        "--batch-seconds",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--cloudwatch-namespace",
        default="WikiLambdaAnalytics",
    )

    return parser.parse_args()


# =========================================================
# S3 HELPERS
# =========================================================

def parse_s3_uri(uri):

    clean = uri.replace(
        "s3://",
        "",
        1,
    )

    bucket, _, prefix = clean.partition("/")

    return bucket, prefix.rstrip("/")


# =========================================================
# KINESIS HELPERS
# =========================================================

def get_shard_ids(
    kinesis,
    stream_name,
):
    """
    Discover Kinesis shards using DescribeStream.

    DescribeStream is used rather than ListShards because
    the restricted EMR lab role already has the required
    permission.
    """

    shard_ids = []

    response = kinesis.describe_stream(
        StreamName=stream_name,
    )

    while True:

        description = response[
            "StreamDescription"
        ]

        for shard in description["Shards"]:

            shard_ids.append(
                shard["ShardId"]
            )

        if not description.get(
            "HasMoreShards"
        ):
            break

        last_shard = (
            description["Shards"][-1][
                "ShardId"
            ]
        )

        response = kinesis.describe_stream(
            StreamName=stream_name,
            ExclusiveStartShardId=last_shard,
        )

    return shard_ids


def create_iterators(
    kinesis,
    stream_name,
    shard_ids,
):

    iterators = {}

    for shard_id in shard_ids:

        response = (
            kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="LATEST",
            )
        )

        iterators[shard_id] = response[
            "ShardIterator"
        ]

        print(
            f"[KINESIS] Iterator created: "
            f"{shard_id}",
            flush=True,
        )

    return iterators


def read_batch(
    kinesis,
    iterators,
):

    records = []

    for shard_id in list(
        iterators.keys()
    ):

        iterator = iterators.get(
            shard_id
        )

        if not iterator:
            continue

        try:

            response = (
                kinesis.get_records(
                    ShardIterator=iterator,
                    Limit=1000,
                )
            )

            records.extend(
                response.get(
                    "Records",
                    [],
                )
            )

            iterators[shard_id] = (
                response.get(
                    "NextShardIterator"
                )
            )

        except Exception as exc:

            print(
                "[KINESIS ERROR] "
                f"shard={shard_id} "
                f"error={repr(exc)}",
                flush=True,
            )

    return records


# =========================================================
# EVENT DECODING
# =========================================================

def decode_record(record):
    """
    Decode a Kinesis Wikimedia record and return
    its article title.

    Returns None for malformed or irrelevant records.
    """

    try:

        raw = record["Data"]

        if isinstance(
            raw,
            (bytes, bytearray),
        ):
            raw = raw.decode(
                "utf-8",
                errors="replace",
            )

        event = json.loads(raw)

        title = event.get("title")

        if not title:
            return None

        return str(title)

    except Exception as exc:

        print(
            f"[PARSE ERROR] {repr(exc)}",
            flush=True,
        )

        return None


# =========================================================
# SPARK AGGREGATION
# =========================================================

def aggregate_titles(
    sc,
    titles,
):

    if not titles:
        return {}

    rdd = sc.parallelize(
        titles
    )

    counts = (
        rdd
        .map(
            lambda title: (
                title,
                1,
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .collect()
    )

    return dict(counts)


# =========================================================
# BATCH-LAYER BASELINE
# =========================================================

def load_historical_baselines(
    spark,
    baseline_s3,
):
    """
    Load historical article edit-rate baselines from
    Batch Layer Parquet output.

    The dataset is loaded once when the speed layer starts.

    Returns:

        {
            "Article A": 4.67,
            "Article B": 12.0,
            ...
        }
    """

    if not baseline_s3:

        print(
            "[BASELINE] No baseline path "
            "configured.",
            flush=True,
        )

        return {}

    try:

        print(
            "[BASELINE] Loading from "
            f"{baseline_s3}",
            flush=True,
        )

        df = (
            spark.read
            .parquet(
                baseline_s3
            )
            .select(
                "title",
                "baseline_edits_per_hour",
            )
            .dropna()
        )

        rows = df.collect()

        baselines = {}

        for row in rows:

            title = row["title"]

            baseline = row[
                "baseline_edits_per_hour"
            ]

            if (
                title is None
                or baseline is None
            ):
                continue

            baseline = float(
                baseline
            )

            if baseline <= 0:
                continue

            baselines[
                str(title)
            ] = baseline

        print(
            "[BASELINE SUCCESS] "
            f"loaded={len(baselines)}",
            flush=True,
        )

        return baselines

    except Exception as exc:

        print(
            "[BASELINE ERROR] "
            f"{repr(exc)}",
            flush=True,
        )

        traceback.print_exc()

        # Do not kill the real-time layer if the
        # batch baseline cannot be loaded.
        return {}


# =========================================================
# LAMBDA COMBINED TREND ANALYTICS
# =========================================================

def build_trending_articles(
    counts,
    baselines,
    window_seconds,
):
    """
    Combine Speed Layer activity with Batch Layer
    historical baselines.

    Example:

        3 edits in a 5-minute window

        current rate =
            3 * (3600 / 300)
            = 36 edits/hour

        historical baseline =
            4.67 edits/hour

        trend score =
            36 / 4.67
            = 7.71

    A trend score > 1 means the article is currently
    receiving edits faster than its historical rate.
    """

    candidates = []

    if window_seconds <= 0:
        window_seconds = 300

    hourly_multiplier = (
        3600.0
        / float(window_seconds)
    )

    for title, count in (
        counts.items()
    ):

        if count < TREND_MIN_EDITS:
            continue

        current_rate = (
            float(count)
            * hourly_multiplier
        )

        baseline = baselines.get(
            title
        )

        if (
            baseline is not None
            and baseline > 0
        ):

            trend_score = (
                current_rate
                / baseline
            )

            candidate = {
                "title": title,
                "recent_edits": int(
                    count
                ),
                "current_edits_per_hour":
                    round(
                        current_rate,
                        2,
                    ),
                "baseline_edits_per_hour":
                    round(
                        baseline,
                        2,
                    ),
                "trend_score":
                    round(
                        trend_score,
                        2,
                    ),
                "baseline_available":
                    True,
            }

        else:

            candidate = {
                "title": title,
                "recent_edits": int(
                    count
                ),
                "current_edits_per_hour":
                    round(
                        current_rate,
                        2,
                    ),
                "baseline_edits_per_hour":
                    None,
                "trend_score":
                    None,
                "baseline_available":
                    False,
            }

        candidates.append(
            candidate
        )

    # -----------------------------------------------------
    # Ranking
    # -----------------------------------------------------
    #
    # Historical articles:
    #     ranked using trend score.
    #
    # New articles:
    #     ranked using current edit rate.
    #
    # Articles with historical evidence are placed first
    # because their trend score has stronger analytical
    # meaning.
    # -----------------------------------------------------

    def ranking_key(item):

        if item[
            "baseline_available"
        ]:

            return (
                1,
                item["trend_score"],
                item["recent_edits"],
            )

        return (
            0,
            item[
                "current_edits_per_hour"
            ],
            item["recent_edits"],
        )

    candidates.sort(
        key=ranking_key,
        reverse=True,
    )

    return candidates[:TOP_N]


# =========================================================
# SNAPSHOT
# =========================================================

def build_snapshot(
    sc,
    window_records,
    baselines,
    window_seconds,
):

    titles = [
        title
        for _, title
        in window_records
    ]

    if not titles:
        return None

    counts = aggregate_titles(
        sc,
        titles,
    )

    trending = (
        build_trending_articles(
            counts=counts,
            baselines=baselines,
            window_seconds=window_seconds,
        )
    )

    snapshot = {

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "window_seconds":
            window_seconds,

        "total_events_in_window":
            len(titles),

        "distinct_articles_in_window":
            len(counts),

        "trending_articles":
            trending,
    }

    return snapshot


# =========================================================
# S3 + CLOUDWATCH OUTPUT
# =========================================================

def publish_snapshot(
    s3,
    cloudwatch,
    bucket,
    prefix,
    namespace,
    snapshot,
):

    if not snapshot:
        return

    body = json.dumps(
        snapshot,
        ensure_ascii=False,
        indent=2,
    ).encode(
        "utf-8"
    )

    latest_key = (
        f"{prefix}/latest.json"
    )

    history_key = (
        f"{prefix}/history/"
        f"{int(time.time())}.json"
    )

    # Latest snapshot
    s3.put_object(
        Bucket=bucket,
        Key=latest_key,
        Body=body,
        ContentType="application/json",
    )

    # Historical speed-layer snapshot
    s3.put_object(
        Bucket=bucket,
        Key=history_key,
        Body=body,
        ContentType="application/json",
    )

    total_events = snapshot[
        "total_events_in_window"
    ]

    trending_count = len(
        snapshot[
            "trending_articles"
        ]
    )

    # CloudWatch operational metrics
    cloudwatch.put_metric_data(

        Namespace=namespace,

        MetricData=[

            {
                "MetricName":
                    "SpeedLayerWindowEvents",
                "Value":
                    float(total_events),
                "Unit":
                    "Count",
            },

            {
                "MetricName":
                    "SpeedLayerTrendingArticleCount",
                "Value":
                    float(trending_count),
                "Unit":
                    "Count",
            },
        ],
    )

    print(
        "[SUCCESS] "
        f"events={total_events} "
        f"articles="
        f"{snapshot['distinct_articles_in_window']} "
        f"trending={trending_count}",
        flush=True,
    )

    print(
        "[S3 SUCCESS] "
        f"s3://{bucket}/{latest_key}",
        flush=True,
    )


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_args()

    print(
        "===============================================",
        flush=True,
    )

    print(
        " Wiki Lambda Analytics - Speed Layer V9",
        flush=True,
    )

    print(
        "===============================================",
        flush=True,
    )

    print(
        f"[CONFIG] stream={args.stream_name}",
        flush=True,
    )

    print(
        f"[CONFIG] region={args.region}",
        flush=True,
    )

    print(
        f"[CONFIG] output={args.output_s3}",
        flush=True,
    )

    print(
        "[CONFIG] "
        f"window={args.window_seconds}s",
        flush=True,
    )

    print(
        "[CONFIG] "
        f"batch={args.batch_seconds}s",
        flush=True,
    )

    # -----------------------------------------------------
    # Spark
    # -----------------------------------------------------

    sc = SparkContext.getOrCreate()

    sc.setLogLevel(
        "WARN"
    )

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    # -----------------------------------------------------
    # AWS clients
    # -----------------------------------------------------

    kinesis = boto3.client(
        "kinesis",
        region_name=args.region,
    )

    s3 = boto3.client(
        "s3",
        region_name=args.region,
    )

    cloudwatch = boto3.client(
        "cloudwatch",
        region_name=args.region,
    )

    # -----------------------------------------------------
    # Output location
    # -----------------------------------------------------

    bucket, prefix = (
        parse_s3_uri(
            args.output_s3
        )
    )

    # -----------------------------------------------------
    # Batch Layer baseline
    # -----------------------------------------------------

    if args.baseline_s3:

        baseline_s3 = (
            args.baseline_s3
        )

    else:

        baseline_s3 = (
            f"s3://{bucket}/"
            "processed/"
            "article_baseline/"
        )

    baselines = (
        load_historical_baselines(
            spark,
            baseline_s3,
        )
    )

    print(
        "[BASELINE CONFIG] "
        f"path={baseline_s3} "
        f"articles={len(baselines)}",
        flush=True,
    )

    # -----------------------------------------------------
    # Kinesis shards
    # -----------------------------------------------------

    print(
        "[KINESIS] Discovering shards...",
        flush=True,
    )

    shard_ids = get_shard_ids(
        kinesis,
        args.stream_name,
    )

    print(
        "[KINESIS] "
        f"shards={len(shard_ids)}",
        flush=True,
    )

    if not shard_ids:

        raise RuntimeError(
            "No Kinesis shards discovered."
        )

    iterators = create_iterators(
        kinesis,
        args.stream_name,
        shard_ids,
    )

    # -----------------------------------------------------
    # Sliding window
    # -----------------------------------------------------

    window_records = deque()

    print(
        "[START] Speed Layer V9 running",
        flush=True,
    )

    # =====================================================
    # STREAMING LOOP
    # =====================================================

    while True:

        batch_start = time.time()

        try:

            # ---------------------------------------------
            # Read Kinesis
            # ---------------------------------------------

            records = read_batch(
                kinesis,
                iterators,
            )

            now = time.time()

            parsed_count = 0

            # ---------------------------------------------
            # Decode events
            # ---------------------------------------------

            for record in records:

                title = decode_record(
                    record
                )

                if not title:
                    continue

                window_records.append(
                    (
                        now,
                        title,
                    )
                )

                parsed_count += 1

            # ---------------------------------------------
            # Sliding-window expiry
            # ---------------------------------------------

            cutoff = (
                now
                - args.window_seconds
            )

            while (
                window_records
                and
                window_records[0][0]
                < cutoff
            ):

                window_records.popleft()

            print(
                "[BATCH] "
                f"received={len(records)} "
                f"parsed={parsed_count} "
                f"window={len(window_records)}",
                flush=True,
            )

            # ---------------------------------------------
            # Build + publish analytics
            # ---------------------------------------------

            if window_records:

                analytics_start = time.perf_counter()

                snapshot = (
                    build_snapshot(
                        sc=sc,
                        window_records=
                            window_records,
                        baselines=
                            baselines,
                        window_seconds=
                            args.window_seconds,
                    )
                )

                publish_snapshot(
                    s3=s3,
                    cloudwatch=cloudwatch,
                    bucket=bucket,
                    prefix=prefix,
                    namespace=
                        args.cloudwatch_namespace,
                    snapshot=snapshot,
                )

                processing_latency_ms = (
                    time.perf_counter()
                    - analytics_start
                ) * 1000.0

                cloudwatch.put_metric_data(
                    Namespace=
                        args.cloudwatch_namespace,
                    MetricData=[
                        {
                            "MetricName":
                                "SpeedLayerProcessingLatencyMs",
                            "Value":
                                processing_latency_ms,
                            "Unit":
                                "Milliseconds",
                        }
                    ],
                )

                print(
                    "[PERFORMANCE] "
                    f"processing_latency_ms="
                    f"{processing_latency_ms:.2f} "
                    f"window_events="
                    f"{len(window_records)}",
                    flush=True,
                )

            else:

                print(
                    "[WINDOW] "
                    "No records currently "
                    "in sliding window",
                    flush=True,
                )

        except KeyboardInterrupt:

            print(
                "[STOP] "
                "Keyboard interrupt",
                flush=True,
            )

            break

        except Exception as exc:

            print(
                "[LOOP ERROR] "
                f"{repr(exc)}",
                flush=True,
            )

            traceback.print_exc()

        # ---------------------------------------------
        # Maintain configured micro-batch interval
        # ---------------------------------------------

        elapsed = (
            time.time()
            - batch_start
        )

        sleep_seconds = max(
            0.0,
            float(
                args.batch_seconds
            ) - elapsed,
        )

        if sleep_seconds > 0:

            time.sleep(
                sleep_seconds
            )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()