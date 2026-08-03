"""
AWS Ingestion Producer.

Reuses the existing SSE reader / parser / filter pipeline, but instead of
dispatching to local in-process workers, publishes each accepted WikiEvent
as a JSON record onto a Kinesis Data Stream.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import boto3

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.producer.connection import ResilientSSEReader
from app.producer.parser import EventParser
from app.producer.processor import EventFilter

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("kinesis_producer")

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "wiki-lambda-stream")
REGION = os.environ.get("AWS_REGION", "us-east-1")
CW_NAMESPACE = os.environ.get("CLOUDWATCH_NAMESPACE", "WikiLambdaAnalytics")
METRIC_PUBLISH_INTERVAL_SECONDS = 10


class KinesisPublisher:
    def __init__(self) -> None:
        self.kinesis = boto3.client("kinesis", region_name=REGION)
        self.cloudwatch = boto3.client("cloudwatch", region_name=REGION)
        self._sent_since_last_metric = 0
        self._last_metric_push = time.monotonic()

    def put_record(self, event_dict: dict) -> None:
        payload = json.dumps(event_dict).encode("utf-8")
        partition_key = event_dict.get("title") or "unknown"
        try:
            self.kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=payload,
                PartitionKey=partition_key[:256],
            )
            self._sent_since_last_metric += 1
        except Exception:
            logger.exception("Failed to put record to Kinesis stream %s", STREAM_NAME)

        self._maybe_publish_metric()

    def _maybe_publish_metric(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_metric_push
        if elapsed < METRIC_PUBLISH_INTERVAL_SECONDS:
            return
        rate = self._sent_since_last_metric / elapsed if elapsed > 0 else 0.0
        try:
            self.cloudwatch.put_metric_data(
                Namespace=CW_NAMESPACE,
                MetricData=[
                    {
                        "MetricName": "IngestionEventsPerSecond",
                        "Value": rate,
                        "Unit": "Count/Second",
                    },
                    {
                        "MetricName": "IngestionEventsPublished",
                        "Value": float(self._sent_since_last_metric),
                        "Unit": "Count",
                    },
                ],
            )
            logger.info("Published CloudWatch metric: %.3f events/sec", rate)
        except Exception:
            logger.exception("Failed to publish CloudWatch metric")
        self._sent_since_last_metric = 0
        self._last_metric_push = now


def main() -> None:
    logger.info("Starting Kinesis ingestion producer -> stream '%s' (region=%s)", STREAM_NAME, REGION)
    reader = ResilientSSEReader()
    parser = EventParser()
    event_filter = EventFilter()
    publisher = KinesisPublisher()

    for raw_payload in reader.events():
        event = parser.parse(raw_payload)
        if event is None:
            continue
        if not event_filter.process(event):
            continue
        publisher.put_record(
            {
                "timestamp": event.to_iso(),
                "title": event.title,
                "user": event.user,
                "anonymous": event.anonymous,
                "bot": event.bot,
                "comment": event.comment,
                "event_type": event.event_type,
                "namespace": event.namespace,
                "wiki": event.wiki,
                "server_name": event.server_name,
            }
        )


if __name__ == "__main__":
    main()