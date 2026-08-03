"""
Batch Layer - PySpark job (runs on EMR, data-parallel across worker nodes).
Reads raw JSON landed in S3 by Firehose, computes historical aggregates,
writes Parquet for Athena.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input-s3", required=True)
    p.add_argument("--output-s3", required=True)
    p.add_argument("--top-n", type=int, default=50)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("WikiLambdaAnalytics-BatchLayer")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    df = spark.read.option("recursiveFileLookup", "true").json(args.input_s3)
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    df = df.withColumn("edit_date", F.to_date("timestamp"))
    df = df.withColumn("edit_hour", F.date_trunc("hour", "timestamp"))
    df.cache()

    total_events = df.count()
    total_articles = df.select("title").distinct().count()
    total_editors = df.select("user").distinct().count()
    print(f"[batch_layer_job] total_events={total_events} total_articles={total_articles} "
          f"total_editors={total_editors}")

    top_articles = (
        df.groupBy("title").agg(F.count("*").alias("edit_count"))
        .orderBy(F.desc("edit_count")).limit(args.top_n)
    )
    top_articles.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/top_articles/")

    bot_vs_human = df.groupBy("bot").agg(F.count("*").alias("edit_count"))
    bot_vs_human.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/bot_vs_human/")

    namespace_breakdown = df.groupBy("namespace").agg(F.count("*").alias("edit_count"))
    namespace_breakdown.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/namespace_breakdown/")

    edit_rate = df.groupBy("edit_hour").agg(F.count("*").alias("edit_count")).orderBy("edit_hour")
    edit_rate.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/edit_rate_hourly/")

    per_article_hourly = df.groupBy("title", "edit_hour").agg(F.count("*").alias("hourly_edits"))
    baseline = (
        per_article_hourly.groupBy("title")
        .agg(F.avg("hourly_edits").alias("baseline_edits_per_hour"))
        .orderBy(F.desc("baseline_edits_per_hour"))
    )
    baseline.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/article_baseline/")

    summary = spark.createDataFrame(
        [(total_events, total_articles, total_editors)],
        ["total_stored_events", "total_articles", "total_editors"],
    )
    summary.write.mode("overwrite").parquet(f"{args.output_s3.rstrip('/')}/summary/")

    print("[batch_layer_job] Done. Parquet outputs written under:", args.output_s3)
    spark.stop()


if __name__ == "__main__":
    main()