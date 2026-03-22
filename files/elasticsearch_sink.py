"""
sinks/elasticsearch_sink.py
Pushes anomaly alerts to Elasticsearch for indexing and search.
Falls back to local JSON log when Elasticsearch is unavailable.
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path

try:
    from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
    ES_AVAILABLE = True
except ImportError:
    ES_AVAILABLE = False

# Config
ES_HOST      = os.getenv("ES_HOST",       "http://localhost:9200")
ES_INDEX     = os.getenv("ES_INDEX",      "streaming-alerts")
ALERT_LOG    = os.getenv("ALERT_LOG",     "/tmp/streaming_output/alerts.jsonl")
_es_client   = None


def _get_client():
    """Get or create Elasticsearch client (singleton)."""
    global _es_client
    if _es_client is None and ES_AVAILABLE:
        try:
            _es_client = Elasticsearch([ES_HOST], request_timeout=5)
            _es_client.info()   # test connection
            print(f"[ES] Connected to Elasticsearch at {ES_HOST}")
        except Exception:
            print(f"[ES] Cannot connect to {ES_HOST} — using file fallback")
            _es_client = None
    return _es_client


def push_alert_to_elasticsearch(alert: dict):
    """
    Push a single anomaly alert to Elasticsearch.
    Falls back to JSONL file if Elasticsearch is unavailable.
    """
    alert["indexed_at"] = datetime.utcnow().isoformat()

    client = _get_client()
    if client:
        try:
            client.index(
                index    = ES_INDEX,
                document = alert,
                id       = alert.get("alert_id"),
            )
            print(f"[ES] Indexed alert: {alert.get('alert_id')} "
                  f"— {alert.get('alert_type')} [{alert.get('severity')}]")
            return
        except Exception as e:
            print(f"[ES] Failed to index: {e}")

    # Fallback — write to JSONL file
    _write_to_file(alert)


def push_metrics_to_elasticsearch(metrics: dict, index: str = "streaming-metrics"):
    """Push aggregated metrics to Elasticsearch for dashboarding."""
    metrics["indexed_at"] = datetime.utcnow().isoformat()

    client = _get_client()
    if client:
        try:
            client.index(index=index, document=metrics)
            return
        except Exception:
            pass

    _write_to_file(metrics, path=ALERT_LOG.replace("alerts", "metrics"))


def search_alerts(
    severity:   str = None,
    alert_type: str = None,
    last_n:     int = 20,
) -> list:
    """
    Search recent alerts from Elasticsearch.
    Used by the dashboard to show alert history.
    """
    client = _get_client()

    if client:
        try:
            query = {"bool": {"must": []}}
            if severity:
                query["bool"]["must"].append({"term": {"severity": severity}})
            if alert_type:
                query["bool"]["must"].append({"term": {"alert_type": alert_type}})
            if not query["bool"]["must"]:
                query = {"match_all": {}}

            result = client.search(
                index = ES_INDEX,
                body  = {
                    "query": query,
                    "sort":  [{"indexed_at": {"order": "desc"}}],
                    "size":  last_n,
                }
            )
            return [hit["_source"] for hit in result["hits"]["hits"]]
        except Exception as e:
            print(f"[ES] Search error: {e}")

    # Fallback — read from file
    return _read_from_file(last_n)


def _write_to_file(data: dict, path: str = ALERT_LOG):
    """Write alert/metrics to local JSONL file."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(data) + "\n")


def _read_from_file(n: int = 20, path: str = ALERT_LOG) -> list:
    """Read last N alerts from JSONL file."""
    if not Path(path).exists():
        return []
    try:
        lines = Path(path).read_text().strip().splitlines()
        recent = lines[-n:]   # last N lines
        return [json.loads(l) for l in reversed(recent)]
    except Exception:
        return []


if __name__ == "__main__":
    # Test the sink
    test_alert = {
        "alert_id":    "test_001",
        "alert_type":  "spike",
        "severity":    "high",
        "metric":      "event_count",
        "current_val": 450,
        "baseline":    100.0,
        "z_score":     3.5,
        "pct_change":  350.0,
        "message":     "Traffic spike: 450 events (350% above baseline 100)",
        "timestamp":   datetime.utcnow().isoformat(),
    }

    print("Pushing test alert...")
    push_alert_to_elasticsearch(test_alert)

    print("\nRetrieving recent alerts...")
    alerts = search_alerts(last_n=5)
    for a in alerts:
        print(f"  [{a.get('severity','?').upper()}] {a.get('message','?')}")
