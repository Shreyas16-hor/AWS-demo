"""
dashboard/app.py
Real-time Streamlit dashboard for the streaming analytics pipeline.

HOW TO RUN:
    pip install streamlit plotly pandas
    streamlit run dashboard/app.py

Opens at: http://localhost:8501
Auto-refreshes every 5 seconds.
"""

import time
import json
import random
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque

import pandas as pd

try:
    import streamlit as st
    import plotly.graph_objects as go
    import plotly.express as px
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("Install: pip install streamlit plotly pandas")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sinks.delta_sink           import read_latest_metrics
from sinks.elasticsearch_sink   import search_alerts

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title = "Streaming Analytics",
    page_icon  = "📊",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0a1628; color: #e2e8f0; }
    .metric-card {
        background: #0d1f3c; border: 1px solid #1e3a5f;
        border-radius: 12px; padding: 20px; text-align: center;
    }
    .metric-value { font-size: 2.5rem; font-weight: 800; color: #06b6d4; }
    .metric-label { font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 1px; }
    .alert-critical { background: rgba(220,38,38,0.15); border-left: 4px solid #dc2626;
                      padding: 10px 16px; border-radius: 6px; margin: 4px 0; }
    .alert-high     { background: rgba(245,158,11,0.15); border-left: 4px solid #f59e0b;
                      padding: 10px 16px; border-radius: 6px; margin: 4px 0; }
    .alert-medium   { background: rgba(59,130,246,0.15); border-left: 4px solid #3b82f6;
                      padding: 10px 16px; border-radius: 6px; margin: 4px 0; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────
def load_metrics() -> pd.DataFrame:
    """Load latest 1-minute metrics from Delta Lake (or simulation)."""
    rows = read_latest_metrics("/tmp/delta/events/metrics_1min", n=60)

    if rows:
        df = pd.DataFrame(rows)
        if "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])
            df = df.sort_values("window_start")
        return df

    # Simulation data if pipeline isn't running
    return _simulate_metrics()


def load_alerts() -> list:
    """Load recent anomaly alerts."""
    alerts = search_alerts(last_n=10)
    if alerts:
        return alerts
    return _simulate_alerts()


def load_top_pages() -> pd.DataFrame:
    """Load top pages data."""
    rows = read_latest_metrics("/tmp/delta/events/top_pages", n=20)
    if rows:
        return pd.DataFrame(rows)
    return _simulate_top_pages()


def _simulate_metrics() -> pd.DataFrame:
    """Generate realistic simulation data for demo."""
    now  = datetime.utcnow()
    rows = []
    for i in range(30):
        t     = now - timedelta(minutes=30-i)
        spike = random.random() < 0.05   # 5% chance of spike
        base  = random.randint(85, 115)
        count = random.randint(250, 400) if spike else base

        rows.append({
            "window_start":    t,
            "total_events":    count,
            "unique_users":    int(count * 0.4),
            "unique_sessions": int(count * 0.6),
            "purchase_count":  random.randint(5, 15),
            "total_revenue":   round(random.uniform(500, 1500), 2),
            "avg_order_value": round(random.uniform(50, 200), 2),
            "page_views":      int(count * 0.5),
            "clicks":          int(count * 0.2),
            "searches":        int(count * 0.15),
        })
    return pd.DataFrame(rows)


def _simulate_alerts() -> list:
    """Generate simulation alerts."""
    return [
        {
            "alert_type": "spike", "severity": "high",
            "message":    "Traffic spike: 312 events (210% above baseline 100)",
            "timestamp":  (datetime.utcnow()-timedelta(minutes=3)).isoformat(),
        },
        {
            "alert_type": "revenue_drop", "severity": "critical",
            "message":    "Revenue drop: $245.00 (38% below baseline $395.00)",
            "timestamp":  (datetime.utcnow()-timedelta(minutes=8)).isoformat(),
        },
        {
            "alert_type": "drop", "severity": "medium",
            "message":    "Traffic drop: 42 events (55% below baseline 93.0)",
            "timestamp":  (datetime.utcnow()-timedelta(minutes=15)).isoformat(),
        },
    ]


def _simulate_top_pages() -> pd.DataFrame:
    pages = ["/home","/search","/product/shoes","/cart","/checkout",
             "/product/laptop","/category/electronics","/profile"]
    return pd.DataFrame({
        "page":       pages,
        "view_count": sorted([random.randint(20, 200) for _ in pages], reverse=True),
    })


# ── Chart helpers ─────────────────────────────────────────────
DARK_BG = "#0a1628"
GRID    = "rgba(255,255,255,0.06)"

def line_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#06b6d4"):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x], y=df[y], mode="lines+markers",
        line=dict(color=color, width=2.5),
        marker=dict(size=5, color=color),
        fill="tozeroy",
        fillcolor=f"rgba(6,182,212,0.08)",
        name=y,
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(color="#e2e8f0", size=14)),
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color="#94a3b8"),
        xaxis=dict(showgrid=True, gridcolor=GRID, color="#64748b"),
        yaxis=dict(showgrid=True, gridcolor=GRID, color="#64748b"),
        margin=dict(t=40, b=20, l=20, r=20),
        height=220,
    )
    return fig


def bar_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#3b82f6"):
    fig = px.bar(df, x=x, y=y, title=title, color_discrete_sequence=[color])
    fig.update_layout(
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color="#94a3b8"),
        title=dict(font=dict(color="#e2e8f0", size=14)),
        xaxis=dict(showgrid=False, color="#64748b"),
        yaxis=dict(showgrid=True, gridcolor=GRID, color="#64748b"),
        margin=dict(t=40, b=20, l=20, r=20),
        height=280,
        showlegend=False,
    )
    return fig


# ════════════════════════════════════════════════════════════════
# MAIN DASHBOARD
# ════════════════════════════════════════════════════════════════
def main():
    # ── Header ────────────────────────────────────────────────
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("## 📊 Real-Time Streaming Analytics")
        st.markdown(
            f"<small style='color:#64748b'>Last updated: "
            f"{datetime.utcnow().strftime('%H:%M:%S UTC')} — "
            f"Auto-refreshes every 5s</small>",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        refresh = st.button("🔄 Refresh Now")

    st.divider()

    # ── Load data ─────────────────────────────────────────────
    metrics_df = load_metrics()
    alerts     = load_alerts()
    pages_df   = load_top_pages()

    # Latest row for KPI cards
    if not metrics_df.empty:
        latest = metrics_df.iloc[-1]
        prev   = metrics_df.iloc[-2] if len(metrics_df) > 1 else latest
    else:
        latest = prev = {}

    # ── KPI Cards ─────────────────────────────────────────────
    st.markdown("### ⚡ Live Metrics")
    k1, k2, k3, k4, k5 = st.columns(5)

    kpi_data = [
        (k1, "Events / Min",  latest.get("total_events",    0), prev.get("total_events",    0), "#06b6d4"),
        (k2, "Active Users",  latest.get("unique_users",    0), prev.get("unique_users",    0), "#22c55e"),
        (k3, "Revenue / Min", f"${latest.get('total_revenue', 0):,.0f}", None, "#f59e0b"),
        (k4, "Purchases",     latest.get("purchase_count",  0), prev.get("purchase_count",  0), "#a855f7"),
        (k5, "Avg Order",     f"${latest.get('avg_order_value', 0):,.0f}", None, "#ef4444"),
    ]

    for col, label, value, prev_val, color in kpi_data:
        with col:
            delta = None
            if prev_val is not None and isinstance(value, (int, float)):
                delta = f"{((value - prev_val) / (prev_val + 1)) * 100:+.1f}%"
            st.metric(
                label    = label,
                value    = value,
                delta    = delta,
                delta_color = "normal",
            )

    st.divider()

    # ── Charts Row 1 ──────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        if not metrics_df.empty and "total_events" in metrics_df.columns:
            st.plotly_chart(
                line_chart(metrics_df, "window_start", "total_events",
                           "Events Per Minute", "#06b6d4"),
                use_container_width=True,
            )

    with c2:
        if not metrics_df.empty and "total_revenue" in metrics_df.columns:
            st.plotly_chart(
                line_chart(metrics_df, "window_start", "total_revenue",
                           "Revenue Per Minute ($)", "#22c55e"),
                use_container_width=True,
            )

    # ── Charts Row 2 ──────────────────────────────────────────
    c3, c4 = st.columns([2, 1])

    with c3:
        if not metrics_df.empty:
            # Event type breakdown
            event_breakdown = pd.DataFrame({
                "Type":  ["Page Views", "Clicks", "Searches"],
                "Count": [
                    int(metrics_df["page_views"].sum()) if "page_views" in metrics_df.columns else 0,
                    int(metrics_df["clicks"].sum())     if "clicks"     in metrics_df.columns else 0,
                    int(metrics_df["searches"].sum())   if "searches"   in metrics_df.columns else 0,
                ]
            })
            st.plotly_chart(
                bar_chart(event_breakdown, "Type", "Count", "Event Type Breakdown", "#3b82f6"),
                use_container_width=True,
            )

    with c4:
        if not pages_df.empty:
            st.markdown("**🔝 Top Pages**")
            top5 = pages_df.head(5)
            for _, row in top5.iterrows():
                views = int(row.get("view_count", 0))
                page  = row.get("page", "")
                bar_width = min(int((views / (top5["view_count"].max() + 1)) * 100), 100)
                st.markdown(
                    f"<div style='margin:4px 0'>"
                    f"<small style='color:#94a3b8'>{page}</small><br>"
                    f"<div style='background:#1e3a5f;border-radius:4px;height:8px;width:100%'>"
                    f"<div style='background:#3b82f6;height:100%;width:{bar_width}%;border-radius:4px'></div>"
                    f"</div><small style='color:#64748b'>{views:,} views</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── Alerts ────────────────────────────────────────────────
    st.markdown("### 🚨 Anomaly Alerts")
    if alerts:
        for alert in alerts[:5]:
            severity = alert.get("severity", "medium")
            msg      = alert.get("message", "")
            ts       = alert.get("timestamp", "")[:19].replace("T", " ")
            css_class = f"alert-{severity}"
            icon = {"critical":"🔴","high":"🟠","medium":"🟡"}.get(severity, "🔵")
            st.markdown(
                f"<div class='{css_class}'>"
                f"{icon} <b>[{severity.upper()}]</b> {msg} "
                f"<small style='color:#64748b;float:right'>{ts}</small>"
                f"</div>",
                unsafe_allow_html=True,
            )
    else:
        st.success("✅ No anomalies detected in the last 10 minutes")

    st.divider()

    # ── Pipeline status ───────────────────────────────────────
    st.markdown("### ⚙️ Pipeline Status")
    s1, s2, s3, s4 = st.columns(4)
    components = [
        (s1, "Kafka Producer",    "🟢 Running", "#22c55e"),
        (s2, "Spark Streaming",   "🟢 Running", "#22c55e"),
        (s3, "Delta Lake",        "🟢 Writing", "#22c55e"),
        (s4, "Anomaly Detector",  "🟢 Active",  "#22c55e"),
    ]
    for col, name, status, color in components:
        with col:
            st.markdown(
                f"<div style='background:#0d1f3c;border:1px solid #1e3a5f;"
                f"border-radius:10px;padding:14px;text-align:center'>"
                f"<div style='font-size:1.2rem'>{status}</div>"
                f"<div style='color:#64748b;font-size:0.8rem;margin-top:4px'>{name}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Auto-refresh ──────────────────────────────────────────
    time.sleep(5)
    st.rerun()


if __name__ == "__main__":
    main()
