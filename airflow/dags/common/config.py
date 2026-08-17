"""Shared constants and helpers for the Streaming DW DAGs.

Values that differ per environment come from Airflow Variables (set via
AIRFLOW_VAR_* env in docker-compose, or the Variables UI). Glue job *names* are
the contract between these DAGs and the not-yet-built Glue jobs under ``glue/``;
keep them in sync as those jobs land.
"""

from __future__ import annotations

import os

from common.notify import notify_failure

# --- Regions (see PLAN_v2.md / streaming-etl/README.md) -----------------
# Data bucket lives in us-west-2; Glue + Athena run in us-east-1 (org SCP blocks
# us-west-2 for our IAM user). Cross-region S3 reads are fine at this scale.
AWS_REGION = "us-east-1"

# --- S3 ------------------------------------------------------------------------
# Read straight from the AIRFLOW_VAR_* env (set in docker-compose) rather than
# Variable.get(): top-level Variable access at DAG-parse time is discouraged in
# Airflow 3 (it would round-trip through the Task Execution API). Same value, no
# parse-time API dependency.
S3_BUCKET = os.environ.get("AIRFLOW_VAR_STREAMING_S3_BUCKET", "acme-dw-streaming-xs2026")
# Iceberg landing table data lives under the pyiceberg warehouse layout
# (<namespace>.db/<table>/data/event_date=*/event_hour=*/...), not the old JSON path.
LANDING_EVENTS_PREFIX = "streaming_landing.db/playback_events/data"

# --- Glue catalog databases ----------------------------------------------------
DB_RAW = "streaming_raw"
DB_PROCESSED = "streaming_processed"
DB_REPORTING = "streaming_reporting"

# --- Glue job names (must match the jobs created under glue/) -------------------
GLUE = {
    "imdb_to_raw": "streaming_imdb_to_raw",
    "raw_events": "streaming_raw_events",
    "raw_snapshots": "streaming_raw_snapshots",
    "dims_refresh": "streaming_processed_dims",
    "fact_playback_events": "streaming_fact_playback_events",
    "fact_view_sessions": "streaming_fact_view_sessions",
    "fact_daily_engagement": "streaming_fact_daily_engagement",
    "reporting_aggregates": "streaming_reporting_aggregates",
    "compaction_landing": "streaming_compaction_landing",
    "compaction_facts": "streaming_compaction_facts",
}

# --- Lambda function names -----------------------------------------------------
LAMBDA_IMDB_MIRROR = "streaming_imdb_mirror"
LAMBDA_HTML_RENDER = "streaming_html_render"

# --- Default args shared by all DAGs -------------------------------------------
DEFAULT_ARGS = {
    "owner": "schumannx",
    "retries": 2,
    "retry_exponential_backoff": True,
    # Email (via SNS) on any task failure — no-op until STREAMING_ALERT_TOPIC_ARN is set.
    "on_failure_callback": notify_failure,
}
