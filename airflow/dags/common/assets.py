"""Airflow Assets used for data-aware cross-DAG scheduling.

Producing an Asset on a task's ``outlets`` lets a downstream DAG schedule on it
instead of a blind cron — see DESIGN.md §4.3. (Datasets were renamed "Assets" in
Airflow 3; the authoring interface now lives under ``airflow.sdk``.)
"""

from __future__ import annotations

from airflow.sdk import Asset

IMDB_RAW = Asset("s3://acme-dw-streaming-xs2026/raw/imdb/")
RAW_EVENTS = Asset("s3://acme-dw-streaming-xs2026/raw/streaming/playback_events/")
