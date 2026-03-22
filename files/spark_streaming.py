"""
consumers/spark_streaming.py
Core PySpark Structured Streaming job.
Reads from Kafka, processes in micro-batches, writes to output sinks.

HOW TO RUN:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \\
        spark_streaming.py

OR with docker:
    docker-compose up
"""

import os
import sys
import json
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.streaming import StreamingQuery
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("[WARNING] PySpark not installed. Running in simulation mode.")
    print("[WARNING] Install: pip install pyspark")

from consumers.aggregations import (
    parse_events, deduplicate_events,
    aggregate_1min_metrics, aggregate_5min_sliding,
    aggregate_top_pages, aggregate_device_breakdown,
    aggregate_revenue_by_minute,
)
from consumers.anomaly_detector import AnomalyDetector
from sinks.delta_sink import write_to_delta
from sinks.elasticsearch_sink import push_alert_to_elasticsearch


# ── Config ────────────────────────────────────────────────────
KAFKA_BROKERS      = os.getenv("KAFKA_BROKERS",   "localhost:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",     "user-events")
CHECKPOINT_DIR     = os.getenv("CHECKPOINT_DIR",  "/tmp/spark-checkpoints")
DELTA_OUTPUT_PATH  = os.getenv("DELTA_PATH",      "/tmp/delta/events")
MICRO_BATCH_SEC    = int(os.getenv("BATCH_SECS",  "5"))


def create_spark_session() -> "SparkSession":
    """Create and configure SparkSession."""
    return (
        SparkSession.builder
        .appName("RealTimeStreamingAnalytics")
        # Kafka integration package
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "io.delta:delta-core_2.12:2.3.0"
        )
        # Delta Lake config
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Streaming config
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "8")    # reduce for local mode
        # Memory
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )


def read_from_kafka(spark: "SparkSession") -> "DataFrame":
    """
    Create a streaming DataFrame that reads from Kafka.
    Returns raw bytes — parse_events() will deserialize.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")     # only new messages
        .option("failOnDataLoss",          "false")      # don't fail if topic deleted
        .option("maxOffsetsPerTrigger",    "10000")      # backpressure limit
        .load()
    )


def process_stream(spark: "SparkSession"):
    """
    Main streaming processing pipeline.
    Sets up multiple output streams from one input.
    """
    print(f"[Spark] Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"[Spark] Micro-batch interval: {MICRO_BATCH_SEC}s")

    # ── Step 1: Read raw stream from Kafka ───────────────────
    raw_stream = read_from_kafka(spark)

    # ── Step 2: Parse + deduplicate ──────────────────────────
    events = deduplicate_events(parse_events(raw_stream))

    # Global anomaly detector (shared across batches)
    detector = AnomalyDetector()

    # ── Step 3: 1-minute aggregations ────────────────────────
    metrics_1min = aggregate_1min_metrics(events)

    # ── Step 4: Write 1-min metrics to Delta Lake ─────────────
    query_delta = (
        metrics_1min.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{MICRO_BATCH_SEC} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/delta_1min")
        .foreachBatch(
            lambda df, batch_id: write_to_delta(
                df, batch_id,
                path=f"{DELTA_OUTPUT_PATH}/metrics_1min",
                partition_cols=["window_start"],
            )
        )
        .start()
    )
    print("[Spark] Stream 1: 1-min metrics → Delta Lake started")

    # ── Step 5: 5-minute sliding window + anomaly detection ───
    metrics_5min = aggregate_5min_sliding(events)

    def detect_and_alert(df, batch_id):
        """foreachBatch handler: run anomaly detection + push alerts."""
        rows = df.collect()
        for row in rows:
            metrics = row.asDict()
            alerts  = detector.check_batch(metrics)
            for alert in alerts:
                print(f"[Anomaly] 🚨 {alert.severity.upper()}: {alert.message}")
                push_alert_to_elasticsearch(alert.__dict__)

        if batch_id % 10 == 0:   # log summary every 10 batches
            summary = detector.get_summary()
            print(f"[Detector] Batch {batch_id}: "
                  f"baseline_mean={summary['event_count_baseline']['mean']}, "
                  f"alerts_total={summary['total_alerts']}")

    query_anomaly = (
        metrics_5min.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{MICRO_BATCH_SEC} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomaly")
        .foreachBatch(detect_and_alert)
        .start()
    )
    print("[Spark] Stream 2: 5-min sliding window → anomaly detection started")

    # ── Step 6: Top pages → console (for dashboard polling) ───
    top_pages = aggregate_top_pages(events)

    query_pages = (
        top_pages.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{MICRO_BATCH_SEC} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/pages")
        .foreachBatch(
            lambda df, bid: write_to_delta(
                df, bid,
                path=f"{DELTA_OUTPUT_PATH}/top_pages",
            )
        )
        .start()
    )
    print("[Spark] Stream 3: Top pages → Delta Lake started")

    # ── Step 7: Revenue stream ────────────────────────────────
    revenue_stream = aggregate_revenue_by_minute(events)

    query_revenue = (
        revenue_stream.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{MICRO_BATCH_SEC} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/revenue")
        .foreachBatch(
            lambda df, bid: write_to_delta(
                df, bid,
                path=f"{DELTA_OUTPUT_PATH}/revenue",
            )
        )
        .start()
    )
    print("[Spark] Stream 4: Revenue stream → Delta Lake started")

    print("\n[Spark] All streams running. Press Ctrl+C to stop.\n")

    # ── Wait for all streams ──────────────────────────────────
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[Spark] Stopping all streams...")
        for q in [query_delta, query_anomaly, query_pages, query_revenue]:
            q.stop()
        print("[Spark] All streams stopped.")


def run_simulation():
    """
    Simulation mode when Spark is not installed.
    Processes events in memory to demonstrate the pipeline logic.
    """
    import random
    from producers.event_generator import generate_event
    from consumers.anomaly_detector import AnomalyDetector

    print("[Simulation] Running pipeline simulation (no Spark required)")
    print("[Simulation] Press Ctrl+C to stop\n")

    detector    = AnomalyDetector()
    batch_num   = 0
    all_metrics = []

    try:
        while True:
            batch_num += 1
            batch_size = random.randint(80, 120)

            # Generate a batch of events
            events = [generate_event() for _ in range(batch_size)]

            # Manual aggregation (simulates Spark window)
            purchases   = [e for e in events if e["event_type"] == "purchase"]
            revenue     = sum(e["amount"] for e in purchases if e["amount"])
            active_users = len(set(e["user_id"] for e in events))

            metrics = {
                "event_count":  batch_size,
                "revenue":      round(revenue, 2),
                "active_users": active_users,
                "purchase_count": len(purchases),
                "window_start": datetime.utcnow().isoformat(),
                "window_end":   datetime.utcnow().isoformat(),
            }

            all_metrics.append(metrics)

            # Simulate a spike every 30 batches
            if batch_num % 30 == 0:
                metrics["event_count"] = random.randint(300, 500)
                print(f"[Simulation] 🔺 SPIKE injected in batch {batch_num}")

            # Run anomaly detection
            alerts = detector.check_batch(metrics)
            for alert in alerts:
                print(f"[Simulation] 🚨 ALERT [{alert.severity.upper()}]: "
                      f"{alert.message}")

            # Print batch summary every 10 batches
            if batch_num % 10 == 0:
                summary = detector.get_summary()
                print(f"[Simulation] Batch {batch_num:3d} | "
                      f"events={metrics['event_count']:4d} | "
                      f"revenue=${metrics['revenue']:8.2f} | "
                      f"users={metrics['active_users']:3d} | "
                      f"alerts={summary['total_alerts']}")

            time.sleep(2)   # 2-second micro-batches in simulation

    except KeyboardInterrupt:
        print(f"\n[Simulation] Stopped after {batch_num} batches")
        summary = detector.get_summary()
        print(f"[Simulation] Final: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    if SPARK_AVAILABLE:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        process_stream(spark)
    else:
        run_simulation()
