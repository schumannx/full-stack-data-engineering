# Glue PySpark jobs — Streaming DW (v2 transform layer)

Glue PySpark + Iceberg-Spark jobs that replace the Athena CTAS scripts in
`../streaming-etl/`. They implement skills #04–#06 plus IMDb load and
Iceberg maintenance. Airflow (`../airflow/dags/`) invokes them by the job names
in `../airflow/dags/common/config.py` — those names are the contract.

## Job ↔ file ↔ DAG map

| Glue job name | File | Invoked by | Athena ancestor |
|---|---|---|---|
| `streaming_imdb_to_raw` | `jobs/imdb_to_raw.py` | imdb_monthly | (new in v2) |
| `streaming_raw_events` | `jobs/raw_events.py` | streaming_microbatch | run_skill_04 (events) |
| `streaming_raw_snapshots` | `jobs/raw_snapshots.py` | daily_rollup | run_skill_04 (snapshots) |
| `streaming_processed_dims` | `jobs/processed_dims.py` | daily_rollup | run_skill_05 (dims) |
| `streaming_fact_playback_events` | `jobs/fact_playback_events.py` | streaming_microbatch | run_skill_05 (fact) |
| `streaming_fact_view_sessions` | `jobs/fact_view_sessions.py` | streaming_microbatch | run_skill_05 (sessions) |
| `streaming_fact_daily_engagement` | `jobs/fact_daily_engagement.py` | daily_rollup | run_skill_05 (daily) |
| `streaming_reporting_aggregates` | `jobs/reporting_aggregates.py` | daily_rollup | run_skill_06 |
| `streaming_compaction_landing` | `jobs/compaction_landing.py` | iceberg_maintenance | (new in v2) |
| `streaming_compaction_facts` | `jobs/compaction_facts.py` | iceberg_maintenance | (new in v2) |

## v2 differences from the Athena port

- **Iceberg everywhere.** Landing is read as an Iceberg table (the Kafka consumer
  writes it), not JSON.gz external tables.
- **MERGE, not rebuild.** Facts MERGE INTO by `event_id` / `session_id` so the
  15-min micro-batches are incremental and idempotent (DESIGN.md §4.5), instead of
  CTAS-rebuilding the whole table each run.
- **Interval-scoped.** Hot-path jobs take `--data_interval_start/--data_interval_end`;
  daily jobs take `--engagement_date`. Re-running a given interval is deterministic.
- **SCD1 surrogate keys are stable.** `processed_dims` upserts dims preserving
  surrogate keys (the Athena version reassigned them each run, which would break
  fact FKs).
- **dim_title from real IMDb.** Built from `streaming_raw.title_basics` (produced by
  `imdb_to_raw`), not the 50 hardcoded mock tconsts.
- **Quarantine is a table.** Invalid rows land in
  `streaming_raw.playback_events_quarantine`, not dropped.

## Shared module

`common.py` builds the SparkSession with the Glue catalog registered as an
Iceberg catalog (`glue_catalog`), parses Glue args, and resolves the run interval.
On Glue 4.0 set the job parameter `--datalake-formats=iceberg`.

## Deploying a job to Glue

Each `jobs/*.py` is a Glue job script. `common.py` must be shipped as an extra
Python file:

```bash
aws s3 cp glue/common.py        s3://acme-dw-streaming-xs2026/glue/common.py
aws s3 cp glue/jobs/raw_events.py s3://acme-dw-streaming-xs2026/glue/jobs/raw_events.py

aws glue create-job --name streaming_raw_events --region us-east-1 \
  --role arn:aws:iam::<acct>:role/role-streaming-raw-writer \
  --glue-version 4.0 --number-of-workers 4 --worker-type G.1X \
  --command '{"Name":"glueetl","ScriptLocation":"s3://acme-dw-streaming-xs2026/glue/jobs/raw_events.py","PythonVersion":"3"}' \
  --default-arguments '{"--datalake-formats":"iceberg","--extra-py-files":"s3://acme-dw-streaming-xs2026/glue/common.py","--enable-glue-datacatalog":""}'
```

Terraform under `../infra/terraform/` will manage job creation + the per-zone IAM
roles; the CLI above documents the shape.

## Validation

`../common/validation.py` holds the cross-zone reconciliation assertions (ported
from `../streaming-etl/validate_etl.py`). `daily_rollup` runs them as a
gating task before the reporting refresh.
