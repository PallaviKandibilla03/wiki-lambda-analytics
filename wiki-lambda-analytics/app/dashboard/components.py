"""
Reusable Streamlit UI-rendering components consumed by dashboard.py.

Each function takes data already fetched from the FastAPI backend (plain
dicts/lists) and renders the corresponding dashboard section.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.dashboard.styles import status_badge_html


def render_health_header(health: Dict | None) -> None:
    """Render the header with producer/speed/batch layer status badges."""
    st.title("\U0001F30D Wiki Lambda Analytics")
    st.caption("Real-time Lambda-architecture analytics over the Wikimedia recent-changes stream")

    if health is None:
        st.error("Unable to reach the API. Is `application.py` running?")
        return

    cols = st.columns(4)
    with cols[0]:
        st.markdown(
            status_badge_html(health["status"] == "healthy", f"Overall: {health['status'].title()}"),
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            status_badge_html(health["producer"]["connected"], "Producer"),
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            status_badge_html(health["speed_layer"]["worker_alive"], "Speed Layer"),
            unsafe_allow_html=True,
        )
    with cols[3]:
        st.markdown(
            status_badge_html(health["batch_layer"]["worker_alive"], "Batch Layer"),
            unsafe_allow_html=True,
        )


def render_live_metrics_grid(live: Dict | None) -> None:
    """Render the live analytics metrics grid: events/sec, window volume, editors, anon %."""
    st.markdown('<div class="section-header"><h3>\U0001F4CA Live Analytics</h3></div>', unsafe_allow_html=True)
    if not live:
        st.info("Waiting for live metrics...")
        return

    cols = st.columns(4)
    cols[0].metric("Events / sec", f"{live['events_per_second']:.2f}")
    cols[1].metric(f"Volume ({live['window_size']}s window)", live["total_events"])
    cols[2].metric("Active Editors", live["unique_editors"])
    cols[3].metric("Anonymous %", f"{live['anonymous_ratio'] * 100:.1f}%")


def render_trending_articles(trending_articles: List[Dict]) -> None:
    """Render the trending articles table and bar chart."""
    st.markdown('<div class="section-header"><h3>\U0001F525 Trending Articles</h3></div>', unsafe_allow_html=True)
    if not trending_articles:
        st.info("No trending articles detected in the current window yet.")
        return

    df = pd.DataFrame(trending_articles)
    col_table, col_chart = st.columns([1, 1])
    with col_table:
        st.dataframe(
            df[["title", "recent_edits", "baseline", "trend_score"]],
            use_container_width=True,
            hide_index=True,
        )
    with col_chart:
        fig = px.bar(
            df.sort_values("trend_score"),
            x="trend_score",
            y="title",
            orientation="h",
            title="Trend Score by Article",
            labels={"trend_score": "Trend Score", "title": "Article"},
        )
        fig.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)


def render_event_stream(events: List[Dict]) -> None:
    """Render the live scrolling event stream table."""
    st.markdown('<div class="section-header"><h3>\U0001F4DC Live Event Stream</h3></div>', unsafe_allow_html=True)
    if not events:
        st.info("No events ingested yet.")
        return
    df = pd.DataFrame(events)
    display_cols = [c for c in ["timestamp", "title", "user", "anonymous", "comment"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True, height=320, hide_index=True)


def render_historical_analytics(history: Dict | None) -> None:
    """Render the historical analytics section using DuckDB aggregates."""
    st.markdown('<div class="section-header"><h3>\U0001F4C8 Historical Analytics</h3></div>', unsafe_allow_html=True)
    if not history:
        st.info("No historical data available yet.")
        return

    top_cols = st.columns(3)
    top_cols[0].metric("Total Stored Events", history.get("total_stored_events", 0))
    top_cols[1].metric("Total Articles", history.get("total_articles", 0))
    top_cols[2].metric("Total Editors", history.get("total_editors", 0))

    col_a, col_b = st.columns(2)

    with col_a:
        bot_human = history.get("bot_vs_human", {})
        if bot_human:
            fig = px.pie(
                names=list(bot_human.keys()),
                values=list(bot_human.values()),
                title="Bot vs Human Edits",
                hole=0.4,
            )
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        namespace_breakdown = history.get("namespace_breakdown", [])
        if namespace_breakdown:
            ns_df = pd.DataFrame(namespace_breakdown)
            fig = px.bar(
                ns_df, x="namespace", y="count", title="Namespace Breakdown",
            )
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

    top_articles = history.get("top_edited_articles", [])
    if top_articles:
        st.subheader("Top Edited Articles (All Time)")
        ta_df = pd.DataFrame(top_articles)
        fig = px.bar(
            ta_df.sort_values("edit_count"), x="edit_count", y="title", orientation="h",
        )
        fig.update_layout(height=400, margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

    edit_rate_history = history.get("edit_rate_history", [])
    if edit_rate_history:
        st.subheader("Edit Rate Over Time")
        er_df = pd.DataFrame(edit_rate_history)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=er_df["bucket"], y=er_df["count"], mode="lines+markers"))
        fig.update_layout(height=320, margin=dict(l=10, r=10, t=20, b=10),
                           xaxis_title="Time Bucket", yaxis_title="Edits")
        st.plotly_chart(fig, use_container_width=True)


def render_system_monitoring(health: Dict | None, system_metrics: Dict | None) -> None:
    """Render system/pipeline performance monitoring: uptime, latency proxy, drop rate."""
    st.markdown('<div class="section-header"><h3>\u2699\ufe0f System & Pipeline Performance</h3></div>', unsafe_allow_html=True)
    if not health or not system_metrics:
        st.info("System metrics unavailable.")
        return

    cols = st.columns(4)
    cols[0].metric("Uptime (s)", f"{health.get('uptime_seconds', 0):.0f}")
    cols[1].metric("CPU %", f"{system_metrics.get('cpu_percent', 0):.1f}")
    cols[2].metric("Memory (MB)", f"{system_metrics.get('memory_rss_mb', 0):.1f}")
    drop_rate = system_metrics.get("stream_statistics", {}).get("drop_rate", 0)
    cols[3].metric("Drop Rate", f"{drop_rate * 100:.2f}%")

    with st.expander("Raw pipeline health JSON"):
        st.json(health)
