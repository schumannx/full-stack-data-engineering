# Streaming ETL

Athena + Iceberg implementation of skills #04, #05, #06.
Validates that the skills are not just documented but actually implementable end-to-end.

Reads JSON.gz events landed by [../streaming-generator/](../streaming-generator/), transforms through 3 zones (raw -> processed -> reporting), produces real Iceberg tables in real S3.

## Files

| File | What it does |
|---|---|
| `config.py` | Shared region/bucket/database constants |
| `athena_runner.py` | boto3 helper to submit + poll Athena queries |
| `setup_glue_athena.py` | One-time: create 4 Glue databases (`streaming_landing`, `streaming_raw`, `streaming_processed`, `streaming_reporting`) |
| `bootstrap_dim_sources.py` | Generate customer_profiles + device_registry snapshots, upload to landing zone |
| `run_skill_04_raw.py` | Skill #04: landing JSON.gz -> raw Parquet (dedup, schema cast, partition projection) |
| `run_skill_05_processed.py` | Skill #05: raw -> Iceberg dim+fact (Kimball model — 4 dims + 3 facts) |
| `run_skill_06_reporting.py` | Skill #06: processed Iceberg -> reporting Iceberg (4 pre-aggregates) |
| `validate_etl.py` | Cross-zone reconciliation checks (10 assertions) |

## Region setup

- **Athena + Glue**: us-east-1 (us-west-2 is blocked for our IAM user by an org SCP)
- **S3 bucket**: `acme-dw-streaming-xs2026` in us-west-2 (where data was originally uploaded)
- **Athena results bucket**: `acme-dw-streaming-xs2026-athena-results` in us-east-1
- Cross-region S3 reads from Athena work fine; cost is negligible at our scale (~50KB)

## Prerequisites

1. AWS configured (`aws configure`) with credentials that have:
   - `AmazonAthenaFullAccess`
   - `AWSGlueConsoleFullAccess`
   - S3 read/write on the data bucket
2. The `acme-dw-streaming-xs2026` bucket already populated by [../streaming-generator/](../streaming-generator/) running first

## Setup (one-time)

```bash
cd streaming-etl
python3 -m venv .venv && source .venv/bin/activate
pip install boto3 numpy

# Create Glue databases + verify Athena works
python setup_glue_athena.py

# Re-upload events to correct landing path (matches skill #03 spec)
aws s3 cp --recursive \
    s3://acme-dw-streaming-xs2026/generator_replay/ \
    s3://acme-dw-streaming-xs2026/landing/streaming/playback_events/

# Bootstrap dim source snapshots
python bootstrap_dim_sources.py
```

## Run end-to-end ETL

```bash
python run_skill_04_raw.py        # ~3 min — landing JSON.gz -> raw Parquet
python run_skill_05_processed.py  # ~50 sec — raw -> Iceberg dim+fact
python run_skill_06_reporting.py  # ~30 sec — processed -> reporting aggregates
python validate_etl.py            # ~30 sec — 10 reconciliation checks
```

Total wall-clock: ~5 minutes for the full pipeline. Total Athena cost: ~$0.05.

## What gets created

### Glue databases (us-east-1)
- `streaming_landing` — external tables on JSON.gz files
- `streaming_raw` — Parquet (dedup, schema cast)
- `streaming_processed` — Iceberg (Kimball dim+fact)
- `streaming_reporting` — Iceberg pre-aggregates

### S3 layout (us-west-2 bucket)
```
s3://acme-dw-streaming-xs2026/
  landing/streaming/
    playback_events/yyyy=*/mm=*/dd=*/hh=*/   (JSON.gz)
    customer_profiles/yyyy=*/mm=*/dd=*/      (JSONL.gz)
    device_registry/yyyy=*/mm=*/dd=*/        (JSONL.gz)
  raw/streaming/
    playback_events/event_date=*/event_hour=*/   (Parquet)
    customer_profiles/                            (Parquet)
    device_registry/                              (Parquet)
  processed/streaming/
    dim_title/             (Iceberg, 50 mock IMDb titles)
    dim_customer/          (Iceberg, SCD1 from raw)
    dim_device/            (Iceberg)
    dim_device_version/    (Iceberg)
    fact_playback_events/  (Iceberg, partitioned by event_date)
    fact_view_sessions/    (Iceberg, partitioned by session_start_date)
    fact_daily_engagement/ (Iceberg, partitioned by engagement_date)
  reporting/streaming/
    content_engagement_daily/   (Iceberg)
    device_engagement_daily/    (Iceberg)
    genre_mix_daily/            (Iceberg)
    title_completion_funnel/    (Iceberg)
```

## Sample queries (after running)

```bash
aws athena start-query-execution --region us-east-1 \
  --query-string "SELECT primary_title, total_watch_seconds, completion_rate
                  FROM streaming_reporting.content_engagement_daily
                  ORDER BY total_watch_seconds DESC
                  LIMIT 10" \
  --result-configuration "OutputLocation=s3://acme-dw-streaming-xs2026-athena-results/results/"
```

Or use Athena console at https://console.aws.amazon.com/athena/ (region us-east-1).

## Scope (what this validates vs what it doesn't)

In scope (skill happy path):
- All Iceberg DDL from skills #05 + #06 actually creates tables in Glue catalog
- All MERGE/INSERT/CTAS patterns succeed against real S3 data
- Cross-zone reconciliation passes (no silent data loss)
- Joins resolve (FK coverage 100%)
- Aggregates roll up consistently (genre_mix sums to 100%, device_total == content_total)

Out of scope (deferred):
- Production scale (1M customers, 50M events/day) — using 100 customers + 838 events
- AWS Glue PySpark jobs — using Athena CTAS instead (sufficient at this scale)
- Real IMDb data download — using 50 hardcoded mock tconsts in `dim_title`
- Redshift hot tier (COPY + Spectrum) — Athena queries the Iceberg tables directly
- Iceberg MERGE INTO patterns — using CTAS rebuild for simplicity (skill #05 spec includes both)
- HTML report generation — verify queries dump JSON/dict instead
- Live micro-batch scheduling — single end-to-end run
- Late event injection (1% lag), duplicates (0.5%) — generator emits happy path only

## Re-running

All scripts are idempotent — `DROP TABLE IF EXISTS` first, then CTAS rebuild. Safe to run any number of times. Each full run costs ~$0.05 on Athena.

## Cleanup

If you want to stop using this:

```bash
# Delete Iceberg tables (also removes data in S3 for managed tables)
python -c "from athena_runner import run_query; from config import *; \
  [run_query(f'DROP TABLE IF EXISTS {db}.\"{t}\"') for db,t in [
    (DB_REPORTING,'content_engagement_daily'),
    (DB_REPORTING,'device_engagement_daily'),
    (DB_REPORTING,'genre_mix_daily'),
    (DB_REPORTING,'title_completion_funnel'),
    (DB_PROCESSED,'fact_daily_engagement'),
    (DB_PROCESSED,'fact_view_sessions'),
    (DB_PROCESSED,'fact_playback_events'),
    (DB_PROCESSED,'dim_device_version'),
    (DB_PROCESSED,'dim_device'),
    (DB_PROCESSED,'dim_customer'),
    (DB_PROCESSED,'dim_title'),
  ]]"

# Drop Glue databases (region must be us-east-1)
aws glue delete-database --name streaming_reporting --region us-east-1
aws glue delete-database --name streaming_processed --region us-east-1
aws glue delete-database --name streaming_raw --region us-east-1
aws glue delete-database --name streaming_landing --region us-east-1

# Clean up S3 (optional)
aws s3 rm --recursive s3://acme-dw-streaming-xs2026/raw/
aws s3 rm --recursive s3://acme-dw-streaming-xs2026/processed/
aws s3 rm --recursive s3://acme-dw-streaming-xs2026/reporting/
```

Athena results bucket and original landing data are preserved.
