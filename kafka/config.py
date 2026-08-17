"""Shared config for the Kafka ingest layer (skills #02 producer + #03 consumer).

Env-overridable so the same code runs on a laptop (local broker) and on the
EC2 broker/consumer. See PLAN_v2.md §3 and DESIGN.md §3.4.
"""

from __future__ import annotations

import os

# --- Kafka ---------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
TOPIC = os.environ.get("KAFKA_TOPIC", "streaming.playback_events")
CONSUMER_GROUP = os.environ.get("KAFKA_GROUP", "landing-writer")
NUM_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "6"))
# 7 days — the topic is the authoritative replay/DR source (DESIGN.md §2.4).
RETENTION_MS = int(os.environ.get("KAFKA_RETENTION_MS", str(7 * 24 * 60 * 60 * 1000)))

# --- Micro-batch flush (consumer) ----------------------------------------------
# One Iceberg append per flush, whichever limit hits first. Keeps file counts
# sane between iceberg_maintenance compactions (DESIGN.md §2.4).
FLUSH_MAX_ROWS = int(os.environ.get("FLUSH_MAX_ROWS", "50000"))
FLUSH_MAX_SECONDS = int(os.environ.get("FLUSH_MAX_SECONDS", "60"))

# --- Iceberg / Glue catalog (landing table) ------------------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")  # Glue catalog region
DATA_REGION = os.environ.get("DATA_REGION", "us-west-2")  # warehouse bucket region (cross-region from the catalog)
S3_BUCKET = os.environ.get("STREAMING_S3_BUCKET", "acme-dw-streaming-xs2026")
WAREHOUSE = f"s3://{S3_BUCKET}/"
GLUE_DATABASE = os.environ.get("LANDING_DB", "streaming_landing")
LANDING_TABLE = os.environ.get("LANDING_TABLE", "playback_events")
TABLE_IDENTIFIER = f"{GLUE_DATABASE}.{LANDING_TABLE}"

SCHEMA_VERSION = 1
