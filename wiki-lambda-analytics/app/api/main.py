"""
FastAPI application exposing the Wiki Lambda Analytics endpoints.

This module only defines the app and its routes; the underlying
ServingLayerService singleton is initialized and wired up by
`application.py` before Uvicorn starts serving requests.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.serving_layer.service import get_service

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Wiki Lambda Analytics API",
    description="Real-time and historical analytics over the Wikimedia recent-changes stream.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["monitoring"])
def health():
    """Return overall pipeline health across producer, dispatcher, speed and batch layers."""
    try:
        return get_service().get_system_health()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/live", tags=["analytics"])
def live_metrics():
    """Return current speed-layer (real-time) metrics, including trending articles."""
    try:
        return get_service().get_live_metrics().model_dump()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/trending", tags=["analytics"])
def trending():
    """Return the current list of trending articles."""
    try:
        return {"trending_articles": get_service().get_trending_articles()}
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/history", tags=["analytics"])
def history(
    bucket_minutes: int = Query(default=5, ge=1, le=60, description="Bucket width in minutes."),
    limit_buckets: int = Query(default=48, ge=1, le=500, description="Number of buckets to return."),
):
    """Return batch-layer historical/aggregate statistics and edit-rate time series."""
    try:
        service = get_service()
        summary = service.get_historical_metrics()
        summary["edit_rate_history"] = service.get_edit_rate_history(bucket_minutes, limit_buckets)
        return summary
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/events", tags=["analytics"])
def events(n: int = Query(default=50, ge=1, le=200, description="Number of recent events to return.")):
    """Return the last N ingested (post-filter) events."""
    try:
        return {"events": get_service().get_recent_events(n)}
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/metrics/system", tags=["monitoring"])
def system_metrics():
    """Return CPU, memory, and stream ingestion statistics."""
    try:
        return get_service().get_system_metrics()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
