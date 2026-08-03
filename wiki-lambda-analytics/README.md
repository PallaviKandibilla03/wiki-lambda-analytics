# Wiki Lambda Analytics

A real-time Wikipedia stream analytics platform built with a **Lambda
Architecture**: a Speed Layer for low-latency real-time metrics, a Batch
Layer (DuckDB) for historical/aggregate analytics, and a Serving Layer
(FastAPI) that unifies both, visualized through a Streamlit dashboard.

## Architecture Overview

```
Wikimedia SSE Stream
        │
        ▼
  Producer (connection.py + parser.py + processor.py)
        │  (validated, filtered WikiEvent)
        ▼
    Dispatcher (fan-out, non-blocking)
     ┌──────┴──────┐
     ▼             ▼
StorageWorker   SpeedWorker
     │             │
     ▼             ▼
  DuckDB      SlidingWindow (5 min default)
 (Batch Layer)  (Speed Layer)
     │             │
     └──────┬──────┘
            ▼
     Serving Layer (FastAPI)
            │
            ▼
   Streamlit Dashboard
```

## 1. Prerequisites

- Python 3.11+
- Internet access to `stream.wikimedia.org`

## 2. Installation

```bash
cd wiki-lambda-analytics
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Running the Backend (Ingestion Pipeline + FastAPI)

From the project root:

```bash
python application.py
```

This single process will:

1. Initialize the DuckDB database at `./data/wiki.db`.
2. Start the `StorageWorker` and `SpeedWorker` background threads.
3. Start the `ProducerThread`, which connects to the Wikimedia
   `recentchange` SSE stream with automatic reconnect/backoff.
4. Launch the FastAPI server (Uvicorn) on `http://localhost:8000`.

You should see log output confirming the SSE connection and Uvicorn
startup. Leave this process running.

### API Documentation

Once running, interactive API docs (Swagger UI) are available at:

```
http://localhost:8000/docs
```

Available endpoints:

| Endpoint          | Description                                         |
|-------------------|------------------------------------------------------|
| `GET /health`     | Pipeline health across producer/speed/batch layers    |
| `GET /live`       | Real-time speed-layer metrics + trending articles     |
| `GET /trending`   | Currently trending articles                            |
| `GET /history`    | Batch-layer historical/aggregate statistics            |
| `GET /events`     | Last N ingested (post-filter) events                   |
| `GET /metrics/system` | CPU, memory, and stream ingestion statistics        |

## 4. Running the Dashboard

In a **second terminal** (with the same virtual environment activated),
from the project root:

```bash
streamlit run app/dashboard/dashboard.py
```

Streamlit will print a local URL (typically `http://localhost:8501`).
Open it in your browser. The dashboard polls the FastAPI backend over
HTTP and auto-refreshes on the interval configured in its sidebar.

> The backend (`application.py`) must already be running for the
> dashboard to display data.

## 5. Configuration

All settings are centralized in `app/config/settings.py` and can be
overridden via environment variables prefixed with `WLA_`, e.g.:

```bash
export WLA_WINDOW_SIZE_SECONDS=600
export WLA_API_PORT=8080
```

or via a `.env` file in the project root.

Key settings:

- `stream_url` — Wikimedia EventStreams source URL.
- `window_size_seconds` — Speed layer sliding window size (default 300s).
- `duckdb_path` — Path to the DuckDB database file.
- `reconnect_initial_backoff_seconds` / `reconnect_max_backoff_seconds` —
  SSE reconnect backoff bounds.
- `filter_event_type` / `filter_namespace` / `filter_exclude_bots` —
  Ingestion filter criteria.
- `trend_score_threshold` / `trend_min_edits_fallback` — Trend detection
  tuning.

## 6. Project Structure

See the module docstrings in each file under `app/` for details on that
component's responsibility. High-level layers:

- `app/producer/` — SSE ingestion, parsing, and filtering.
- `app/workers/` — Dispatcher fan-out and per-layer worker threads.
- `app/storage/` + `app/batch_layer/` — DuckDB persistence and analytical queries.
- `app/speed_layer/` — Sliding window, real-time metrics, trend detection.
- `app/serving_layer/` — Unified service coordinating both layers.
- `app/api/` — FastAPI HTTP interface.
- `app/dashboard/` — Streamlit UI.
- `app/monitoring/` — Cross-cutting pipeline statistics.

## 7. Stopping the Platform

Press `Ctrl+C` in the terminal running `application.py` to gracefully
stop the producer and worker threads. Stop the Streamlit dashboard with
`Ctrl+C` in its own terminal.

## 8. Troubleshooting

- **No data in the dashboard**: confirm `application.py` is running and
  reachable at the URL configured in `dashboard_api_base_url`.
- **`database is locked` errors**: DuckDB access is serialized internally
  via a lock in `DuckDBWrapper`; ensure only one `application.py` process
  is running against the same `duckdb_path`.
- **SSE connection drops frequently**: check network/firewall access to
  `stream.wikimedia.org`; the producer will automatically retry with
  exponential backoff.
