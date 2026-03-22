"""
consumers/anomaly_detector.py
Z-score based anomaly detection on event streams.
Detects spikes and drops in event counts vs rolling average.
"""

import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# ── Config ────────────────────────────────────────────────────
Z_SCORE_THRESHOLD  = 2.5    # alert if Z-score > 2.5 (99.4% confidence)
MIN_SAMPLES        = 10     # need at least 10 data points before detecting
DROP_THRESHOLD     = 0.40   # alert if count drops >40% vs rolling average
SPIKE_THRESHOLD    = 3.0    # alert if count spikes >3x vs rolling average
WINDOW_SIZE        = 20     # rolling window size for baseline calculation
REVENUE_DROP_PCT   = 0.30   # alert if revenue drops >30%


@dataclass
class AnomalyAlert:
    """Represents a detected anomaly."""
    alert_id:    str
    alert_type:  str        # "spike" | "drop" | "revenue_drop" | "zero_events"
    severity:    str        # "low" | "medium" | "high" | "critical"
    metric:      str        # what metric triggered this
    current_val: float      # current value
    baseline:    float      # expected baseline
    z_score:     float      # how many std deviations away
    pct_change:  float      # % change from baseline
    message:     str        # human-readable description
    timestamp:   str = field(default_factory=lambda: datetime.utcnow().isoformat())
    window_start: Optional[str] = None
    window_end:   Optional[str] = None


class RollingStats:
    """
    Maintains a rolling window of values and computes
    mean and standard deviation incrementally.
    """

    def __init__(self, window_size: int = WINDOW_SIZE):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)

    def add(self, value: float):
        self.values.append(value)

    @property
    def mean(self) -> float:
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)

    @property
    def std(self) -> float:
        if len(self.values) < 2:
            return 0.0
        m = self.mean
        variance = sum((v - m) ** 2 for v in self.values) / len(self.values)
        return math.sqrt(variance)

    @property
    def count(self) -> int:
        return len(self.values)

    def z_score(self, value: float) -> float:
        """How many standard deviations is value from the mean?"""
        if self.std == 0:
            return 0.0
        return (value - self.mean) / self.std

    def pct_change(self, value: float) -> float:
        """Percentage change from mean. Negative = drop, positive = spike."""
        if self.mean == 0:
            return 0.0
        return (value - self.mean) / self.mean


class AnomalyDetector:
    """
    Stateful anomaly detector that maintains rolling baselines
    and checks each new data point against them.

    Used in the foreachBatch() callback of PySpark Structured Streaming.
    """

    def __init__(self):
        self.event_count_stats  = RollingStats(WINDOW_SIZE)
        self.revenue_stats      = RollingStats(WINDOW_SIZE)
        self.active_user_stats  = RollingStats(WINDOW_SIZE)
        self.alerts_raised      = []
        self.batch_count        = 0

    def check_batch(self, metrics: dict) -> list:
        """
        Check a single batch of aggregated metrics for anomalies.

        Args:
            metrics: Dict with keys: event_count, revenue, active_users,
                     window_start, window_end

        Returns:
            List of AnomalyAlert objects (empty if no anomalies)
        """
        alerts = []
        self.batch_count += 1

        event_count  = metrics.get("event_count",  0)
        revenue      = metrics.get("revenue",       0.0)
        active_users = metrics.get("active_users",  0)
        w_start      = metrics.get("window_start",  "")
        w_end        = metrics.get("window_end",    "")

        # ── Check event count anomaly ─────────────────────────
        if self.event_count_stats.count >= MIN_SAMPLES:
            z     = self.event_count_stats.z_score(event_count)
            pct   = self.event_count_stats.pct_change(event_count)
            baseline = self.event_count_stats.mean

            # Spike detection
            if z > Z_SCORE_THRESHOLD and pct > SPIKE_THRESHOLD - 1:
                alerts.append(AnomalyAlert(
                    alert_id    = f"spike_{int(time.time())}",
                    alert_type  = "spike",
                    severity    = "high" if z > 4 else "medium",
                    metric      = "event_count",
                    current_val = event_count,
                    baseline    = round(baseline, 1),
                    z_score     = round(z, 2),
                    pct_change  = round(pct * 100, 1),
                    message     = f"Traffic spike: {event_count} events "
                                  f"({pct*100:.0f}% above baseline {baseline:.0f})",
                    window_start = w_start,
                    window_end   = w_end,
                ))

            # Drop detection
            elif pct < -DROP_THRESHOLD:
                severity = "critical" if pct < -0.70 else "high" if pct < -0.50 else "medium"
                alerts.append(AnomalyAlert(
                    alert_id    = f"drop_{int(time.time())}",
                    alert_type  = "drop",
                    severity    = severity,
                    metric      = "event_count",
                    current_val = event_count,
                    baseline    = round(baseline, 1),
                    z_score     = round(z, 2),
                    pct_change  = round(pct * 100, 1),
                    message     = f"Traffic drop: {event_count} events "
                                  f"({abs(pct)*100:.0f}% below baseline {baseline:.0f})",
                    window_start = w_start,
                    window_end   = w_end,
                ))

        # ── Check revenue drop ────────────────────────────────
        if self.revenue_stats.count >= MIN_SAMPLES and revenue > 0:
            rev_pct = self.revenue_stats.pct_change(revenue)
            if rev_pct < -REVENUE_DROP_PCT:
                alerts.append(AnomalyAlert(
                    alert_id    = f"rev_drop_{int(time.time())}",
                    alert_type  = "revenue_drop",
                    severity    = "critical" if rev_pct < -0.50 else "high",
                    metric      = "revenue",
                    current_val = round(revenue, 2),
                    baseline    = round(self.revenue_stats.mean, 2),
                    z_score     = round(self.revenue_stats.z_score(revenue), 2),
                    pct_change  = round(rev_pct * 100, 1),
                    message     = f"Revenue drop: ${revenue:.2f} "
                                  f"({abs(rev_pct)*100:.0f}% below baseline "
                                  f"${self.revenue_stats.mean:.2f})",
                    window_start = w_start,
                    window_end   = w_end,
                ))

        # ── Check zero events (possible pipeline failure) ─────
        if event_count == 0 and self.batch_count > 3:
            alerts.append(AnomalyAlert(
                alert_id    = f"zero_{int(time.time())}",
                alert_type  = "zero_events",
                severity    = "critical",
                metric      = "event_count",
                current_val = 0,
                baseline    = self.event_count_stats.mean,
                z_score     = -99.0,
                pct_change  = -100.0,
                message     = "CRITICAL: Zero events received — possible pipeline failure!",
                window_start = w_start,
                window_end   = w_end,
            ))

        # ── Update rolling baselines ──────────────────────────
        self.event_count_stats.add(event_count)
        if revenue > 0:
            self.revenue_stats.add(revenue)
        if active_users > 0:
            self.active_user_stats.add(active_users)

        # Store alerts
        self.alerts_raised.extend(alerts)
        return alerts

    def get_summary(self) -> dict:
        """Return current baseline statistics."""
        return {
            "batches_processed": self.batch_count,
            "total_alerts":      len(self.alerts_raised),
            "event_count_baseline": {
                "mean":    round(self.event_count_stats.mean,  1),
                "std":     round(self.event_count_stats.std,   1),
                "samples": self.event_count_stats.count,
            },
            "revenue_baseline": {
                "mean":    round(self.revenue_stats.mean,  2),
                "std":     round(self.revenue_stats.std,   2),
                "samples": self.revenue_stats.count,
            },
        }


# ── Standalone test ───────────────────────────────────────────
if __name__ == "__main__":
    import random
    import json

    print("Testing AnomalyDetector...\n")
    detector = AnomalyDetector()

    # Feed normal data to build baseline
    print("Phase 1: Building baseline (20 normal batches)...")
    for i in range(20):
        metrics = {
            "event_count":  random.randint(90, 110),
            "revenue":      random.uniform(800, 1200),
            "active_users": random.randint(40, 60),
            "window_start": f"2025-01-01T00:{i:02d}:00",
            "window_end":   f"2025-01-01T00:{i:02d}:05",
        }
        alerts = detector.check_batch(metrics)

    print(f"Baseline built: {json.dumps(detector.get_summary(), indent=2)}\n")

    # Simulate a traffic spike
    print("Phase 2: Simulating traffic SPIKE...")
    spike_metrics = {
        "event_count":  450,    # 4x normal
        "revenue":      3200.0,
        "active_users": 180,
        "window_start": "2025-01-01T00:21:00",
        "window_end":   "2025-01-01T00:21:05",
    }
    alerts = detector.check_batch(spike_metrics)
    if alerts:
        for a in alerts:
            print(f"  🚨 ALERT [{a.severity.upper()}]: {a.message}")
    else:
        print("  No alerts (baseline may need more data)")

    # Simulate a traffic drop
    print("\nPhase 3: Simulating traffic DROP...")
    drop_metrics = {
        "event_count":  15,    # very low
        "revenue":      50.0,
        "active_users": 5,
        "window_start": "2025-01-01T00:22:00",
        "window_end":   "2025-01-01T00:22:05",
    }
    alerts = detector.check_batch(drop_metrics)
    if alerts:
        for a in alerts:
            print(f"  🚨 ALERT [{a.severity.upper()}]: {a.message}")
    else:
        print("  No alerts")

    print(f"\nFinal summary: {json.dumps(detector.get_summary(), indent=2)}")
