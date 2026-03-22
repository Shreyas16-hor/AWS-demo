"""
tests/test_aggregations.py
Unit tests for aggregation logic and anomaly detection.
Run: python -m pytest tests/ -v
"""

import pytest
import sys
import os
import json
import random
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumers.anomaly_detector import (
    AnomalyDetector, RollingStats,
    Z_SCORE_THRESHOLD, DROP_THRESHOLD, MIN_SAMPLES
)
from producers.event_generator import generate_event, EVENT_TYPES


# ════════════════════════════════════════════════════════════════
# Tests for RollingStats
# ════════════════════════════════════════════════════════════════

class TestRollingStats:

    def test_empty_stats(self):
        """New stats object should return zeros."""
        stats = RollingStats()
        assert stats.mean  == 0.0
        assert stats.std   == 0.0
        assert stats.count == 0

    def test_single_value(self):
        """Single value — mean equals value, std is zero."""
        stats = RollingStats()
        stats.add(100)
        assert stats.mean  == 100.0
        assert stats.std   == 0.0
        assert stats.count == 1

    def test_mean_calculation(self):
        """Mean should be correct."""
        stats = RollingStats()
        for v in [10, 20, 30, 40, 50]:
            stats.add(v)
        assert stats.mean == 30.0

    def test_std_calculation(self):
        """Standard deviation should be correct."""
        stats = RollingStats()
        for v in [2, 4, 4, 4, 5, 5, 7, 9]:
            stats.add(v)
        assert round(stats.std, 2) == 2.0

    def test_rolling_window(self):
        """Window should only keep last N values."""
        stats = RollingStats(window_size=5)
        for v in range(10):    # add 10 values to window of 5
            stats.add(v)
        assert stats.count == 5
        # Last 5 values are 5,6,7,8,9 → mean = 7
        assert stats.mean == 7.0

    def test_z_score_positive(self):
        """Value much higher than mean should have positive Z-score."""
        stats = RollingStats()
        for _ in range(20):
            stats.add(100)    # stable baseline
        z = stats.z_score(500)
        assert z > Z_SCORE_THRESHOLD

    def test_z_score_negative(self):
        """Value much lower than mean should have negative Z-score."""
        stats = RollingStats()
        for _ in range(20):
            stats.add(100)
        z = stats.z_score(10)
        assert z < -Z_SCORE_THRESHOLD

    def test_pct_change_increase(self):
        """Increase should give positive pct change."""
        stats = RollingStats()
        for _ in range(5):
            stats.add(100)
        pct = stats.pct_change(150)
        assert abs(pct - 0.5) < 0.01    # 50% increase

    def test_pct_change_decrease(self):
        """Decrease should give negative pct change."""
        stats = RollingStats()
        for _ in range(5):
            stats.add(100)
        pct = stats.pct_change(60)
        assert abs(pct - (-0.4)) < 0.01  # 40% decrease

    def test_zero_mean_no_crash(self):
        """Z-score and pct_change should not crash when mean is 0."""
        stats = RollingStats()
        assert stats.z_score(50)    == 0.0
        assert stats.pct_change(50) == 0.0


# ════════════════════════════════════════════════════════════════
# Tests for AnomalyDetector
# ════════════════════════════════════════════════════════════════

class TestAnomalyDetector:

    def _build_baseline(self, detector: AnomalyDetector, n: int = MIN_SAMPLES + 5):
        """Helper — feed normal data to build baseline."""
        for i in range(n):
            detector.check_batch({
                "event_count":  random.randint(90, 110),
                "revenue":      random.uniform(800, 1200),
                "active_users": random.randint(40, 60),
                "window_start": f"2025-01-01T00:{i:02d}:00",
                "window_end":   f"2025-01-01T00:{i:02d}:05",
            })

    def test_no_alerts_before_min_samples(self):
        """Should not alert before MIN_SAMPLES batches."""
        detector = AnomalyDetector()
        alerts   = detector.check_batch({
            "event_count": 10000,   # extreme spike
            "revenue": 0,
            "active_users": 1,
        })
        assert len(alerts) == 0

    def test_spike_detected(self):
        """Should detect traffic spike after baseline is built."""
        detector = AnomalyDetector()
        self._build_baseline(detector)

        # Inject a massive spike
        alerts = detector.check_batch({
            "event_count":  1000,   # 10x normal
            "revenue":      5000,
            "active_users": 400,
            "window_start": "2025-01-01T01:00:00",
            "window_end":   "2025-01-01T01:01:00",
        })

        spike_alerts = [a for a in alerts if a.alert_type == "spike"]
        assert len(spike_alerts) > 0, "Spike should be detected"
        assert spike_alerts[0].severity in ["medium", "high", "critical"]

    def test_drop_detected(self):
        """Should detect traffic drop after baseline is built."""
        detector = AnomalyDetector()
        self._build_baseline(detector)

        # Inject a severe drop
        alerts = detector.check_batch({
            "event_count":  5,    # ~95% drop
            "revenue":      10.0,
            "active_users": 2,
            "window_start": "2025-01-01T01:00:00",
            "window_end":   "2025-01-01T01:01:00",
        })

        drop_alerts = [a for a in alerts if a.alert_type == "drop"]
        assert len(drop_alerts) > 0, "Drop should be detected"

    def test_revenue_drop_detected(self):
        """Should detect significant revenue drop."""
        detector = AnomalyDetector()
        self._build_baseline(detector)

        alerts = detector.check_batch({
            "event_count":  100,
            "revenue":      50.0,   # ~95% revenue drop
            "active_users": 40,
        })

        rev_alerts = [a for a in alerts if a.alert_type == "revenue_drop"]
        assert len(rev_alerts) > 0, "Revenue drop should be detected"
        assert rev_alerts[0].severity in ["high", "critical"]

    def test_zero_events_critical(self):
        """Zero events after warmup should trigger critical alert."""
        detector = AnomalyDetector()
        self._build_baseline(detector, n=5)

        alerts = detector.check_batch({
            "event_count":  0,
            "revenue":      0,
            "active_users": 0,
        })

        zero_alerts = [a for a in alerts if a.alert_type == "zero_events"]
        assert len(zero_alerts) > 0
        assert zero_alerts[0].severity == "critical"

    def test_normal_traffic_no_alerts(self):
        """Normal traffic should not trigger alerts."""
        detector = AnomalyDetector()
        self._build_baseline(detector)

        alert_count = 0
        for _ in range(10):
            alerts = detector.check_batch({
                "event_count":  random.randint(92, 108),   # within normal range
                "revenue":      random.uniform(850, 1150),
                "active_users": random.randint(42, 58),
            })
            alert_count += len(alerts)

        assert alert_count == 0, f"Normal traffic should have 0 alerts, got {alert_count}"

    def test_alert_has_required_fields(self):
        """Alerts should have all required fields."""
        detector = AnomalyDetector()
        self._build_baseline(detector)

        alerts = detector.check_batch({
            "event_count": 5000, "revenue": 0, "active_users": 0
        })

        for alert in alerts:
            assert hasattr(alert, "alert_id")
            assert hasattr(alert, "alert_type")
            assert hasattr(alert, "severity")
            assert hasattr(alert, "message")
            assert hasattr(alert, "current_val")
            assert hasattr(alert, "baseline")
            assert hasattr(alert, "z_score")
            assert hasattr(alert, "pct_change")
            assert alert.severity in ["low", "medium", "high", "critical"]

    def test_batch_count_increments(self):
        """Batch counter should increment correctly."""
        detector = AnomalyDetector()
        for i in range(5):
            detector.check_batch({"event_count": 100, "revenue": 1000, "active_users": 40})
        assert detector.batch_count == 5


# ════════════════════════════════════════════════════════════════
# Tests for Event Generator
# ════════════════════════════════════════════════════════════════

class TestEventGenerator:

    def test_event_has_required_fields(self):
        """Generated events should have all required schema fields."""
        event = generate_event()
        required = ["event_id", "user_id", "event_type", "timestamp", "page",
                    "session_id", "device", "amount"]
        for field in required:
            assert field in event, f"Missing field: {field}"

    def test_event_id_is_unique(self):
        """Each event should have a unique ID."""
        ids = {generate_event()["event_id"] for _ in range(100)}
        assert len(ids) == 100

    def test_purchase_has_amount(self):
        """Purchase events must have a non-null amount."""
        events = [generate_event() for _ in range(500)]
        purchases = [e for e in events if e["event_type"] == "purchase"]
        for p in purchases:
            assert p["amount"] is not None
            assert p["amount"] > 0

    def test_non_purchase_no_amount(self):
        """Non-purchase events should have null amount."""
        events = [generate_event() for _ in range(500)]
        non_purchases = [e for e in events if e["event_type"] != "purchase"]
        for e in non_purchases:
            assert e["amount"] is None

    def test_valid_event_type(self):
        """All events should have a valid event type."""
        for _ in range(100):
            e = generate_event()
            assert e["event_type"] in EVENT_TYPES

    def test_valid_device(self):
        """Device should be mobile, desktop, or tablet."""
        for _ in range(100):
            e = generate_event()
            assert e["device"] in ["mobile", "desktop", "tablet"]

    def test_timestamp_is_iso(self):
        """Timestamp should be a valid ISO 8601 string."""
        e = generate_event()
        ts = e["timestamp"]
        assert "T" in ts
        assert ts.endswith("Z") or "+" in ts

    def test_event_distribution(self):
        """page_view should be the most common event type."""
        events = [generate_event() for _ in range(1000)]
        page_views = sum(1 for e in events if e["event_type"] == "page_view")
        purchases  = sum(1 for e in events if e["event_type"] == "purchase")
        assert page_views > purchases * 3   # page views >> purchases


# ════════════════════════════════════════════════════════════════
# Integration test — full pipeline simulation
# ════════════════════════════════════════════════════════════════

class TestPipelineIntegration:

    def test_pipeline_simulation(self):
        """
        End-to-end simulation:
        Generate events → aggregate → detect anomalies.
        """
        detector = AnomalyDetector()
        total_alerts = 0

        # Phase 1: Build baseline
        for i in range(MIN_SAMPLES + 5):
            events  = [generate_event() for _ in range(random.randint(85, 115))]
            purchases = [e for e in events if e["event_type"] == "purchase"]
            metrics = {
                "event_count":  len(events),
                "revenue":      sum(e["amount"] for e in purchases if e["amount"]),
                "active_users": len(set(e["user_id"] for e in events)),
            }
            detector.check_batch(metrics)

        # Phase 2: Inject anomaly
        spike_events = [generate_event() for _ in range(500)]
        spike_metrics = {
            "event_count":  500,
            "revenue":      sum(e.get("amount") or 0 for e in spike_events),
            "active_users": len(set(e["user_id"] for e in spike_events)),
        }
        alerts = detector.check_batch(spike_metrics)
        total_alerts += len(alerts)

        # Phase 3: Verify pipeline state
        summary = detector.get_summary()
        assert summary["batches_processed"] == MIN_SAMPLES + 6
        assert summary["event_count_baseline"]["samples"] > 0
        assert total_alerts > 0, "Should have detected the spike"


if __name__ == "__main__":
    # Run tests without pytest
    print("Running manual tests...\n")

    t1 = TestRollingStats()
    for method in [m for m in dir(t1) if m.startswith("test_")]:
        try:
            getattr(t1, method)()
            print(f"  ✅ RollingStats.{method}")
        except AssertionError as e:
            print(f"  ❌ RollingStats.{method}: {e}")

    t2 = TestAnomalyDetector()
    for method in [m for m in dir(t2) if m.startswith("test_")]:
        try:
            getattr(t2, method)()
            print(f"  ✅ AnomalyDetector.{method}")
        except AssertionError as e:
            print(f"  ❌ AnomalyDetector.{method}: {e}")

    t3 = TestEventGenerator()
    for method in [m for m in dir(t3) if m.startswith("test_")]:
        try:
            getattr(t3, method)()
            print(f"  ✅ EventGenerator.{method}")
        except AssertionError as e:
            print(f"  ❌ EventGenerator.{method}: {e}")

    print("\nAll tests completed!")
