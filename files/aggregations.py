"""
consumers/aggregations.py
Window functions and metric calculations for the streaming pipeline.
Used by spark_streaming.py to compute real-time analytics.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, LongType
)


# ── Event Schema ──────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    True),
    StructField("user_id",    StringType(),    True),
    StructField("event_type", StringType(),    True),
    StructField("timestamp",  StringType(),    True),   # ISO string from Kafka
    StructField("page",       StringType(),    True),
    StructField("session_id", StringType(),    True),
    StructField("device",     StringType(),    True),
    StructField("amount",     DoubleType(),    True),   # nullable — only purchases
])


# ── Window Definitions ────────────────────────────────────────
WINDOW_1MIN  = "1 minute"
WINDOW_5MIN  = "5 minutes"
SLIDE_30SEC  = "30 seconds"
WATERMARK    = "10 seconds"    # late data tolerance


def parse_events(raw_df: DataFrame) -> DataFrame:
    """
    Parse raw Kafka messages into structured events.
    - Deserializes JSON value
    - Converts ISO timestamp string to proper Timestamp type
    - Adds event_date column for partitioning
    """
    parsed = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("timestamp"))    # ISO → Timestamp
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_ts"))          # for partitioning in Delta Lake
        )
        .filter(F.col("event_id").isNotNull())    # drop malformed events
        .filter(F.col("user_id").isNotNull())
    )
    return parsed


def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Remove duplicate events using event_id.
    Uses dropDuplicates within each micro-batch.
    For cross-batch deduplication, Delta Lake merge handles it.
    """
    return df.dropDuplicates(["event_id"])


def aggregate_1min_metrics(df: DataFrame) -> DataFrame:
    """
    1-minute tumbling window aggregations.
    Answers: What happened in the last minute?

    Returns:
        window_start, window_end,
        total_events, unique_users, unique_sessions,
        purchase_count, total_revenue, avg_order_value,
        page_views, clicks, searches
    """
    return (
        df
        .withWatermark("event_ts", WATERMARK)
        .groupBy(
            F.window("event_ts", WINDOW_1MIN)
        )
        .agg(
            F.count("*")                           .alias("total_events"),
            F.countDistinct("user_id")             .alias("unique_users"),
            F.countDistinct("session_id")          .alias("unique_sessions"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))
                                                   .alias("purchase_count"),
            F.sum(F.coalesce(F.col("amount"), F.lit(0.0)))
                                                   .alias("total_revenue"),
            F.avg(F.when(F.col("event_type") == "purchase", F.col("amount")))
                                                   .alias("avg_order_value"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))
                                                   .alias("page_views"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))
                                                   .alias("clicks"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))
                                                   .alias("searches"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end")  .alias("window_end"),
            "total_events", "unique_users", "unique_sessions",
            "purchase_count",
            F.round("total_revenue",   2).alias("total_revenue"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "page_views", "clicks", "searches",
        )
    )


def aggregate_5min_sliding(df: DataFrame) -> DataFrame:
    """
    5-minute sliding window with 30-second slide.
    Answers: What's the rolling trend over the last 5 minutes?

    Each output row covers a 5-min window, updated every 30 seconds.
    Used for anomaly detection (comparing windows).
    """
    return (
        df
        .withWatermark("event_ts", WATERMARK)
        .groupBy(
            F.window("event_ts", WINDOW_5MIN, SLIDE_30SEC)
        )
        .agg(
            F.count("*")                .alias("event_count"),
            F.countDistinct("user_id")  .alias("active_users"),
            F.sum(F.coalesce(F.col("amount"), F.lit(0.0)))
                                        .alias("revenue"),
            # Revenue per event type breakdown
            *[
                F.sum(
                    F.when(F.col("event_type") == et, 1).otherwise(0)
                ).alias(f"count_{et.replace('_', '')}")
                for et in ["page_view", "click", "purchase", "search"]
            ]
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end")  .alias("window_end"),
            "event_count", "active_users",
            F.round("revenue", 2).alias("revenue"),
            "count_pageview", "count_click",
            "count_purchase", "count_search",
        )
    )


def aggregate_top_pages(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Top N pages by view count in 1-minute windows.
    Used to populate the 'Top Pages' section of the dashboard.
    """
    return (
        df
        .filter(F.col("event_type") == "page_view")
        .withWatermark("event_ts", WATERMARK)
        .groupBy(
            F.window("event_ts", WINDOW_1MIN),
            "page"
        )
        .agg(
            F.count("*").alias("view_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            "page",
            "view_count",
        )
    )


def aggregate_device_breakdown(df: DataFrame) -> DataFrame:
    """
    Event count broken down by device type per minute.
    Used for the device distribution chart.
    """
    return (
        df
        .withWatermark("event_ts", WATERMARK)
        .groupBy(
            F.window("event_ts", WINDOW_1MIN),
            "device"
        )
        .agg(
            F.count("*")               .alias("event_count"),
            F.countDistinct("user_id") .alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "device", "event_count", "unique_users",
        )
    )


def aggregate_revenue_by_minute(df: DataFrame) -> DataFrame:
    """
    Revenue timeline — revenue per 1-minute bucket.
    Used for the revenue trend line chart.
    """
    return (
        df
        .filter(F.col("event_type") == "purchase")
        .filter(F.col("amount").isNotNull())
        .withWatermark("event_ts", WATERMARK)
        .groupBy(
            F.window("event_ts", WINDOW_1MIN)
        )
        .agg(
            F.sum("amount")  .alias("revenue"),
            F.count("*")     .alias("order_count"),
            F.avg("amount")  .alias("avg_order"),
            F.max("amount")  .alias("max_order"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.round("revenue",   2).alias("revenue"),
            "order_count",
            F.round("avg_order", 2).alias("avg_order"),
            F.round("max_order", 2).alias("max_order"),
        )
    )
