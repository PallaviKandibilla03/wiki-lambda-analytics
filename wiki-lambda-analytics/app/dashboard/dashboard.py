"""
Wiki Lambda Analytics - Live AWS Lambda Architecture Dashboard.

Dashboard responsibilities:
- Display Speed Layer V9 real-time analytics.
- Display Batch Layer historical analytics.
- Present the combined Lambda Architecture trend result.
- Automatically refresh the real-time section every 5 seconds.

The analytical combination of current activity and historical baseline
is performed by Speed Layer V9. The dashboard is intentionally kept
as a presentation layer and does not recompute trend scores.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import streamlit as st


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Wiki Lambda Analytics",
    page_icon="🌐",
    layout="wide",
)


# =========================================================
# AWS CONFIGURATION
# =========================================================

REGION = "us-east-1"
BUCKET = "wiki-lambda-x23352370"

SPEED_LAYER_KEY = "speed_layer_output_v9/latest.json"

s3 = boto3.client(
    "s3",
    region_name=REGION,
)


# =========================================================
# HELPERS
# =========================================================

def read_speed_layer():
    """
    Read the most recent Speed Layer V9 snapshot from S3.
    """

    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=SPEED_LAYER_KEY,
        )

        body = obj["Body"].read()

        return json.loads(body)

    except Exception as exc:
        st.error(
            f"Unable to read speed layer: {exc}"
        )

        return {}


def read_parquet(prefix):
    """
    Read all Parquet objects underneath an S3 prefix and
    combine them into a Pandas DataFrame.

    Every S3 object uses a unique temporary file. This
    prevents Windows file-locking collisions when several
    dashboard sections read Parquet data.
    """

    try:
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=prefix,
        )

        parquet_objects = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".parquet")
        ]

        if not parquet_objects:
            return pd.DataFrame()

        frames = []

        for key in parquet_objects:

            temp_handle = tempfile.NamedTemporaryFile(
                suffix=".parquet",
                delete=False,
            )

            local_file = Path(
                temp_handle.name
            )

            temp_handle.close()

            try:
                s3.download_file(
                    BUCKET,
                    key,
                    str(local_file),
                )

                frame = pd.read_parquet(
                    local_file
                )

                if not frame.empty:
                    frames.append(
                        frame
                    )

            finally:
                try:
                    local_file.unlink(
                        missing_ok=True
                    )
                except OSError:
                    pass

        if not frames:
            return pd.DataFrame()

        return pd.concat(
            frames,
            ignore_index=True,
        )

    except Exception as exc:

        st.warning(
            f"Unable to read {prefix}: {exc}"
        )

        return pd.DataFrame()


# =========================================================
# STATIC HEADER
# =========================================================

st.title("🌐 Wiki Lambda Analytics")

st.caption(
    "AWS Lambda Architecture for real-time and historical "
    "analytics over Wikimedia Recent Changes"
)


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.header("AWS Pipeline")

    st.write(
        f"Region: `{REGION}`"
    )

    st.write(
        f"S3 Bucket: `{BUCKET}`"
    )

    st.markdown("### Architecture")

    st.markdown(
        """
        Wikimedia EventStream  
        ↓  
        Kinesis  
        ↓  
        Firehose → S3 Raw Data  
        ↓  
        Speed Layer: Spark Streaming  
        ↓  
        Batch Baseline + Speed Analytics  
        ↓  
        S3 + CloudWatch

        Batch Layer: EMR PySpark  
        ↓  
        Parquet → Glue → Athena
        """
    )


# =========================================================
# LIVE SPEED LAYER
# =========================================================

@st.fragment(run_every=5)
def live_dashboard():
    """
    Real-time portion of the dashboard.

    Streamlit reruns this fragment approximately every
    five seconds without rerunning the complete dashboard.
    """

    speed = read_speed_layer()

    # -----------------------------------------------------
    # SPEED LAYER HEADER
    # -----------------------------------------------------

    st.header(
        "⚡ Speed Layer — Real-Time Analytics"
    )

    if not speed:

        st.warning(
            "No speed-layer snapshot is currently available."
        )

        return

    # -----------------------------------------------------
    # METRICS
    # -----------------------------------------------------

    col1, col2, col3 = st.columns(3)

    total_events = speed.get(
        "total_events_in_window",
        0,
    )

    distinct_articles = speed.get(
        "distinct_articles_in_window",
        0,
    )

    trending = speed.get(
        "trending_articles",
        [],
    )

    col1.metric(
        "Events in Sliding Window",
        f"{total_events:,}",
    )

    col2.metric(
        "Distinct Articles",
        f"{distinct_articles:,}",
    )

    col3.metric(
        "Trending Articles",
        len(trending),
    )

    # -----------------------------------------------------
    # SNAPSHOT INFORMATION
    # -----------------------------------------------------

    generated = speed.get(
        "generated_at",
        "N/A",
    )

    window_seconds = speed.get(
        "window_seconds",
        300,
    )

    window_minutes = (
        window_seconds / 60
        if window_seconds
        else 5
    )

    st.caption(
        f"Latest speed-layer snapshot: {generated}"
    )

    # -----------------------------------------------------
    # TRENDING ARTICLES
    # -----------------------------------------------------

    if trending:

        st.subheader(
            "🔥 Trending Articles — Lambda Combined View"
        )

        st.caption(
            "Real-time activity is compared with historical "
            "article baselines generated by the batch layer."
        )

        trending_df = pd.DataFrame(
            trending
        )

        required_columns = {
            "title",
            "recent_edits",
            "current_edits_per_hour",
            "baseline_edits_per_hour",
            "trend_score",
            "baseline_available",
        }

        if required_columns.issubset(
            trending_df.columns
        ):

            # ---------------------------------------------
            # SPEED + BATCH ALREADY COMBINED BY V9
            # ---------------------------------------------

            display_df = trending_df[
                [
                    "title",
                    "recent_edits",
                    "current_edits_per_hour",
                    "baseline_edits_per_hour",
                    "trend_score",
                    "baseline_available",
                ]
            ].copy()

            # ---------------------------------------------
            # SORT
            # ---------------------------------------------

            display_df = display_df.sort_values(
                by=[
                    "baseline_available",
                    "trend_score",
                    "current_edits_per_hour",
                ],
                ascending=[
                    False,
                    False,
                    False,
                ],
                na_position="last",
            )

            # ---------------------------------------------
            # CURRENT EDIT RATE
            # ---------------------------------------------

            display_df[
                "current_edits_per_hour"
            ] = pd.to_numeric(
                display_df[
                    "current_edits_per_hour"
                ],
                errors="coerce",
            ).round(1)

            # ---------------------------------------------
            # HISTORICAL BASELINE
            #
            # IMPORTANT:
            # Keep every value as a string so Arrow does
            # not receive a mixture of float and str.
            # ---------------------------------------------

            display_df[
                "baseline_edits_per_hour"
            ] = display_df[
                "baseline_edits_per_hour"
            ].apply(
                lambda value:
                    "No historical data"
                    if pd.isna(value)
                    else f"{float(value):.2f}"
            ).astype(str)

            # ---------------------------------------------
            # TREND SCORE
            #
            # Also explicitly stored as strings because
            # values such as "N/A" and "36.00×" coexist.
            # ---------------------------------------------

            display_df[
                "trend_score"
            ] = display_df[
                "trend_score"
            ].apply(
                lambda value:
                    "N/A"
                    if pd.isna(value)
                    else f"{float(value):.2f}×"
            ).astype(str)

            # Internal processing flag is unnecessary
            # for dashboard users.

            display_df = display_df.drop(
                columns=[
                    "baseline_available",
                ]
            )

            # ---------------------------------------------
            # USER-FRIENDLY COLUMN NAMES
            # ---------------------------------------------

            display_df = display_df.rename(
                columns={
                    "title":
                        "Article",

                    "recent_edits":
                        "Recent Edits",

                    "current_edits_per_hour":
                        "Current Edits/Hour",

                    "baseline_edits_per_hour":
                        "Historical Baseline/Hour",

                    "trend_score":
                        "Trend vs Baseline",
                }
            )

            # ---------------------------------------------
            # DISPLAY TABLE
            # ---------------------------------------------

            st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
            )

            st.caption(
                "Trend vs Baseline = current edit rate ÷ "
                "historical edit rate. "
                "A score above 1× indicates activity above "
                "the article's historical baseline. "
                "Articles without sufficient historical "
                "data are shown as N/A."
            )

        else:

            st.warning(
                "The speed-layer snapshot does not contain "
                "all expected V9 analytical fields."
            )

            st.dataframe(
                trending_df,
                width="stretch",
                hide_index=True,
            )

    else:

        st.info(
            "No articles currently exceed the "
            "trending threshold."
        )

    # -----------------------------------------------------
    # WINDOW INFORMATION
    # -----------------------------------------------------

    st.caption(
        f"Sliding window: {window_minutes:g} minutes"
    )

    # -----------------------------------------------------
    # REFRESH INDICATOR
    # -----------------------------------------------------

    st.caption(
        "🔄 Dashboard auto-refreshes every 5 seconds · "
        f"Last UI refresh: "
        f"{datetime.now().strftime('%H:%M:%S')}"
    )


# Start the independently refreshing speed-layer fragment.

live_dashboard()


# =========================================================
# BATCH LAYER
# =========================================================

st.header(
    "🗄️ Batch Layer — Historical Analytics"
)


# =========================================================
# HISTORICAL SUMMARY
# =========================================================

summary = read_parquet(
    "processed/summary/"
)

if not summary.empty:

    expected_summary_columns = [
        "total_stored_events",
        "total_articles",
        "total_editors",
    ]

    available_summary_columns = [
        column
        for column in expected_summary_columns
        if column in summary.columns
    ]

    if available_summary_columns:

        summary = summary.dropna(
            subset=available_summary_columns,
            how="all",
        )

    if not summary.empty:

        row = summary.iloc[0]

        c1, c2, c3 = st.columns(3)

        if "total_stored_events" in row:

            value = row[
                "total_stored_events"
            ]

            if pd.notna(value):

                c1.metric(
                    "Historical Events",
                    f"{int(value):,}",
                )

        if "total_articles" in row:

            value = row[
                "total_articles"
            ]

            if pd.notna(value):

                c2.metric(
                    "Historical Articles",
                    f"{int(value):,}",
                )

        if "total_editors" in row:

            value = row[
                "total_editors"
            ]

            if pd.notna(value):

                c3.metric(
                    "Historical Editors",
                    f"{int(value):,}",
                )

else:

    st.info(
        "Historical summary data is not currently available."
    )


# =========================================================
# MOST EDITED HISTORICAL ARTICLES
# =========================================================

st.subheader(
    "📊 Most Edited Historical Articles"
)

top_articles = read_parquet(
    "processed/top_articles/"
)

if (
    not top_articles.empty
    and "title" in top_articles.columns
    and "edit_count" in top_articles.columns
):

    top_articles = (
        top_articles
        .dropna(
            subset=[
                "title",
                "edit_count",
            ]
        )
        .sort_values(
            "edit_count",
            ascending=False,
        )
        .head(10)
    )

    if not top_articles.empty:

        st.bar_chart(
            top_articles,
            x="title",
            y="edit_count",
        )

else:

    st.info(
        "Historical top-article data is not "
        "currently available."
    )


# =========================================================
# HISTORICAL ARTICLE BASELINES
# =========================================================

st.subheader(
    "📈 Historical Article Baselines"
)

baseline = read_parquet(
    "processed/article_baseline/"
)

if (
    not baseline.empty
    and "title" in baseline.columns
    and "baseline_edits_per_hour" in baseline.columns
):

    baseline_display = baseline[
        [
            "title",
            "baseline_edits_per_hour",
        ]
    ].copy()

    baseline_display = (
        baseline_display
        .dropna(
            subset=[
                "title",
                "baseline_edits_per_hour",
            ]
        )
        .sort_values(
            "baseline_edits_per_hour",
            ascending=False,
        )
        .head(10)
    )

    baseline_display[
        "baseline_edits_per_hour"
    ] = pd.to_numeric(
        baseline_display[
            "baseline_edits_per_hour"
        ],
        errors="coerce",
    ).round(2)

    baseline_display = baseline_display.rename(
        columns={
            "title":
                "Article",

            "baseline_edits_per_hour":
                "Historical Edits/Hour",
        }
    )

    st.dataframe(
        baseline_display,
        width="stretch",
        hide_index=True,
    )

    st.caption(
        "Historical baselines are generated by the "
        "EMR PySpark batch layer and are used by Speed "
        "Layer V9 to identify unusually high current "
        "editing activity."
    )

else:

    st.info(
        "Historical article baseline data is not "
        "currently available."
    )


# =========================================================
# AWS LAMBDA ARCHITECTURE
# =========================================================

st.header(
    "☁️ AWS Lambda Architecture"
)

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Kinesis",
    "Ingestion",
)

c2.metric(
    "S3",
    "Persistent Storage",
)

c3.metric(
    "Spark / EMR",
    "Analytics",
)

c4.metric(
    "Athena",
    "Query Layer",
)


st.info(
    """
    **Speed Layer:** Wikimedia Recent Changes events are
    ingested through Amazon Kinesis and processed using
    Spark on Amazon EMR. A five-minute sliding window
    provides near-real-time article activity.

    **Batch Layer:** Historical events stored in Amazon S3
    are processed using EMR PySpark to generate historical
    article statistics and baseline edit rates.

    **Combined View:** Speed Layer V9 compares the current
    normalized edit rate with the historical batch-layer
    baseline to calculate a relative trend score.

    **Serving / Query Layer:** Analytical outputs are stored
    in Amazon S3, historical Parquet datasets are catalogued
    through AWS Glue and queried using Amazon Athena, while
    this Streamlit dashboard presents the resulting views.
    """
)


# =========================================================
# FOOTER
# =========================================================

st.caption(
    "Wiki Lambda Analytics • AWS Lambda Architecture"
)