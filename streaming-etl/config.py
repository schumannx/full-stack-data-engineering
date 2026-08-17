"""
Shared config for the streaming ETL.

Note on regions: org SCP blocks Athena in us-west-2 for our IAM user, so Athena/Glue
run in us-east-1. The S3 data bucket stays in us-west-2 (where it was created).
Cross-region S3 reads from Athena work fine; cost is negligible at our scale.
"""

# Region split: Athena/Glue in us-east-1, S3 in us-west-2
ATHENA_REGION = "us-east-1"
GLUE_REGION = "us-east-1"
S3_REGION = "us-west-2"

# S3 paths
DATA_BUCKET = "acme-dw-streaming-xs2026"           # us-west-2 — already has data
RESULTS_BUCKET = "acme-dw-streaming-xs2026-athena-results"  # us-east-1 — Athena query results
ATHENA_RESULTS_LOCATION = f"s3://{RESULTS_BUCKET}/results/"

# Glue database names (one per zone)
DB_LANDING = "streaming_landing"
DB_RAW = "streaming_raw"
DB_PROCESSED = "streaming_processed"
DB_REPORTING = "streaming_reporting"

# S3 paths per zone (all under DATA_BUCKET)
LANDING_PATH = f"s3://{DATA_BUCKET}/landing/streaming/"
RAW_PATH = f"s3://{DATA_BUCKET}/raw/streaming/"
PROCESSED_PATH = f"s3://{DATA_BUCKET}/processed/streaming/"
REPORTING_PATH = f"s3://{DATA_BUCKET}/reporting/streaming/"

# Athena workgroup (use default 'primary' for now)
ATHENA_WORKGROUP = "primary"
