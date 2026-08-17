# Streaming DW — dbt marts (dbt-athena)

The **reporting/marts layer** as dbt models, replacing the SQL-shaped PySpark in
`glue/jobs/reporting_aggregates.py`. The hybrid boundary:

- **Spark/Glue** still builds the heavy facts + dims (`fact_view_sessions`, `dim_title`,
  `dim_device`). dbt reads them as **sources** (`models/reporting/_sources.yml`).
- **dbt** builds the four marts on top, as Iceberg incremental tables in `streaming_reporting`:
  `content_engagement_daily`, `device_engagement_daily`, `genre_mix_daily`,
  `title_completion_funnel`.

## Why dbt here
Those four marts were `groupBy().agg().join()` PySpark — i.e. SQL in disguise. As dbt models
you get the MERGE/materialization for free (`config(materialized='incremental', ...)`), plus
declarative tests (`_reporting.yml`) and lineage — no hand-written `MERGE INTO`, no
`table_exists()` branching.

## Run it
```bash
# 1. install (adapter + dbt-utils)
pip install "dbt-athena-community"      # or: dbt-athena
cd dbt/streaming
dbt deps                                # installs dbt_utils (packages.yml)

# 2. credentials: dbt-athena uses your AWS env/role (same as the Glue jobs).
#    The profile is profiles.yml in this dir, so pass --profiles-dir .

# 3. full build of all days present in fact_view_sessions
dbt build --profiles-dir .

# 4. incremental rebuild of a single day (mirrors the Glue --engagement_date arg)
dbt build --profiles-dir . --vars '{engagement_date: "2026-05-31"}'
```
`dbt build` = run models + run tests. Use `dbt run` / `dbt test` to split them.

## ⚠️ Cross-region caveat (read before first run)
Athena runs in **us-east-1**; the Iceberg warehouse bucket is **us-west-2** — the same split
the Glue jobs hit (they needed `s3.cross-region-access-enabled` / Glue 5.0). Athena writing
**Iceberg** to a cross-region bucket is the untested part here. **Smoke-test ONE model first:**
```bash
dbt run --profiles-dir . --select content_engagement_daily
```
If it 301s / fails on the cross-region write, the clean fix is to point `s3_data_dir` (in
`profiles.yml`) at a **us-east-1** bucket for the dbt marts, or co-locate the reporting
warehouse in us-east-1. Reads from us-west-2 are fine; it's the Iceberg *write* to watch.

## Replaces / coexists with
These models write the SAME four `streaming_reporting` tables the Glue
`reporting_aggregates` job writes. Run **one or the other** for a given day — don't run both
against the same partition. To cut over, stop scheduling the Glue reporting step and let dbt
own the marts.

## Next step (not in this project yet): Cosmos in Airflow
Orchestrating these via `astronomer-cosmos` (one `DbtTaskGroup` that renders each model + test
as an Airflow task) is the planned follow-up — see `examples/orchestration-demo/03_cosmos_daily_rollup.py`
for the shape. It needs the cosmos + dbt-athena deps in the Airflow image and CI handling for
parse-time rendering, so it's a separate change.
