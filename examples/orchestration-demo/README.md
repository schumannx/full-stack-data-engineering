# Orchestration demo — YAML (dag-factory) vs dbt + Cosmos

These files all rebuild **your existing `airflow/dags/daily_rollup.py`** three ways so you
can *see* the trade-offs instead of taking my word for it. Nothing here is wired into
Airflow — read them, then delete the folder.

## The files

| File | What it shows |
|------|----------------|
| `01_dagfactory_daily_rollup.yml`  | `daily_rollup` rewritten as dag-factory YAML |
| `01_dagfactory_loader.py`         | The Python you STILL need for the YAML to work |
| `02_dbt_content_engagement_daily.sql` | One PySpark transform → a dbt model |
| `02_dbt_schema.yml`               | Your `validation.py` asserts → dbt tests |
| `03_cosmos_daily_rollup.py`       | `daily_rollup` with the marts run by Cosmos |

## What to notice (the whole point)

1. **YAML doesn't delete the Python.** Open `01_..._daily_rollup.yml` and look at the
   `validate_etl` task — it can't hold logic, so it points at a `.py` file. That file is
   `01_dagfactory_loader.py`, and your 40 lines of `validate_etl` still live there.
   You didn't remove Python; you split it across two files and lost IDE/type help.

2. **YAML struggles with your advanced bits.** See the comment on `schedule` in the YAML:
   your real DAG uses `DatasetOrTimeSchedule(CronTriggerTimetable(...), datasets=[RAW_EVENTS])`.
   dag-factory can't express that combo cleanly — it degrades to a plain cron.

3. **dbt is where Python actually disappears.** Compare `02_dbt_content_engagement_daily.sql`
   (≈18 lines of SQL) against the `content` block in
   `../../glue/jobs/reporting_aggregates.py` (≈15 lines of PySpark). Same result, but the
   dbt one ALSO gives you the MERGE/materialization, tests, and lineage for free via the
   `config()` block — no hand-written `MERGE INTO`.

4. **Cosmos = "maintain DAG as well as dbt".** In `03_cosmos_daily_rollup.py` the four marts
   become a single `DbtTaskGroup` that Cosmos expands into one task per model automatically.
   You keep Spark for the heavy `dims` job; you stop hand-wiring the reporting tasks.
