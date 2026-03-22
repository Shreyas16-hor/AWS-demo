"""
sinks/delta_sink.py
Writes processed streaming data to Delta Lake tables.
Delta Lake provides ACID transactions, time travel, and schema evolution.
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path

try:
    from pyspark.sql import DataFrame
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False

# Fallback: write to JSON files when Delta is not available
FALLBACK_PATH = os.getenv("FALLBACK_PATH", "/tmp/streaming_output")


def write_to_delta(
    df:             "DataFrame",
    batch_id:       int,
    path:           str,
    partition_cols: list = None,
    merge_key:      str  = None,
):
    """
    Write a micro-batch DataFrame to Delta Lake.
    Used as the foreachBatch handler in PySpark Structured Streaming.

    Args:
        df:             The micro-batch DataFrame
        batch_id:       Spark's batch identifier (for logging)
        path:           Delta Lake table path
        partition_cols: Columns to partition by (e.g. ['event_date'])
        merge_key:      If set, do UPSERT instead of append (for deduplication)
    """
    if df.rdd.isEmpty():
        return   # skip empty batches

    count = df.count()

    if not DELTA_AVAILABLE:
        _write_to_json_fallback(df, batch_id, path, count)
        return

    try:
        writer = (
            df.write
            .format("delta")
            .mode("append")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        if merge_key and DeltaTable.isDeltaTable(df.sparkSession, path):
            # UPSERT — prevent duplicate rows
            delta_table = DeltaTable.forPath(df.sparkSession, path)
            (
                delta_table.alias("existing")
                .merge(
                    df.alias("new"),
                    f"existing.{merge_key} = new.{merge_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"[Delta] Batch {batch_id}: UPSERT {count} rows → {path}")
        else:
            writer.save(path)
            print(f"[Delta] Batch {batch_id}: APPEND {count} rows → {path}")

    except Exception as e:
        print(f"[Delta] ERROR batch {batch_id}: {e}")
        # Fallback to JSON on error
        _write_to_json_fallback(df, batch_id, path, count)


def _write_to_json_fallback(df, batch_id: int, path: str, count: int):
    """
    Fallback writer — saves to JSON files when Delta is unavailable.
    Used during development / when Delta is not installed.
    """
    output_dir = Path(FALLBACK_PATH) / Path(path).name
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"batch_{batch_id:06d}_{timestamp}.json"

    try:
        # Convert DataFrame rows to list of dicts
        rows = [row.asDict() for row in df.collect()]

        # Convert non-serializable types
        for row in rows:
            for k, v in row.items():
                if hasattr(v, 'isoformat'):        # datetime
                    row[k] = v.isoformat()
                elif hasattr(v, 'start'):          # Spark Row/Window
                    row[k] = str(v)

        with open(output_file, "w") as f:
            json.dump({"batch_id": batch_id, "count": count, "rows": rows}, f, indent=2)

        print(f"[Delta→JSON] Batch {batch_id}: {count} rows → {output_file}")

    except Exception as e:
        print(f"[Delta→JSON] ERROR: {e}")


def read_latest_metrics(path: str, n: int = 100) -> list:
    """
    Read the latest N records from a Delta table.
    Used by the Streamlit dashboard to display current metrics.

    Returns list of dicts (works with both Delta and JSON fallback).
    """
    if DELTA_AVAILABLE:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                df = (
                    spark.read.format("delta").load(path)
                    .orderBy("window_start", ascending=False)
                    .limit(n)
                )
                return [row.asDict() for row in df.collect()]
        except Exception:
            pass

    # Fallback — read JSON files
    return _read_json_fallback(path, n)


def _read_json_fallback(path: str, n: int) -> list:
    """Read latest records from JSON fallback files."""
    output_dir = Path(FALLBACK_PATH) / Path(path).name
    if not output_dir.exists():
        return []

    all_rows = []
    # Read files sorted by name (newest last due to timestamp in name)
    files = sorted(output_dir.glob("*.json"), reverse=True)

    for f in files[:10]:   # read last 10 batch files
        try:
            data = json.loads(f.read_text())
            all_rows.extend(data.get("rows", []))
        except Exception:
            continue

    # Return latest n rows sorted by window_start
    all_rows.sort(key=lambda x: x.get("window_start", ""), reverse=True)
    return all_rows[:n]
