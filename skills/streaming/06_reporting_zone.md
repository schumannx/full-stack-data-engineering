# Skill: Reporting Zone — Pre-Aggregated Iceberg + Redshift Serving

## Purpose
Defines the reporting layer of the `streaming` real-time analytics pipeline — the final stage end-users actually query. Daily Glue PySpark jobs read the dimensional model from skill #05 (`fact_view_sessions`, `fact_daily_engagement`, `dim_title`, `dim_customer`, `dim_device`) and produce five denormalised pre-aggregate Iceberg tables in `reporting/streaming/`. Those aggregates are then split into a hot tier (last 7 days COPY-loaded into Redshift Serverless for sub-second dashboards) and a cold tier (Spectrum external tables reading the same Iceberg files for unbounded history). HTML reports built on top of the aggregates are published as `html_report` items in the LeastAction catalog and surfaced through chat.

---

## Shared Spec

| Item | Value |
|---|---|
| Bucket | `s3://acme-dw-streaming-xs2026/` |
| Reporting zone path | `reporting/streaming/<table_name>/` |
| HTML report path | `s3://acme-dw-streaming-xs2026/reporting/html_reports/<report_name>/yyyy=*/mm=*/dd=*/` |
| Iceberg format | Apache Iceberg v2 (cold/historical) |
| Redshift cluster | Redshift Serverless |
| Redshift database | `streaming` |
| Redshift schema (hot) | `reporting` |
| Redshift schema (cold) | `reporting_external` (Spectrum) |
| Hot retention | last 7 days |
| Compute | Glue PySpark (10 DPU) for aggregation; Redshift COPY/UNLOAD for hot/cold; Athena + Python for HTML |
| Aggregation cadence | daily 02:30 UTC (after `fact_daily_engagement` at 02:00) |
| Report cadence | daily 03:00 UTC and later (per report) |
| AWS account ID | `123456789012` (placeholder — substitute your own) |
| Spectrum IAM role | `arn:aws:iam::123456789012:role/RedshiftSpectrumRole` |

---

## Architecture Diagram

```
processed/ Iceberg (skill #05)
    │
    ├── fact_view_sessions ────┐
    ├── fact_daily_engagement ─┼─► Glue PySpark daily aggregator
    ├── dim_title ─────────────┤        │
    ├── dim_customer ──────────┘        ▼
    │                            reporting/ Iceberg
    │                                    │
    │                                    ├── content_engagement_daily
    │                                    ├── title_completion_funnel
    │                                    ├── device_engagement_daily
    │                                    ├── cohort_retention_weekly
    │                                    └── genre_mix_daily
    │                                    │
    │                              ┌─────┴─────┐
    │                              ▼           ▼
    │                         Spectrum    COPY (hot 7d)
    │                              │           │
    │                              └─────┬─────┘
    │                                    ▼
    │                           Redshift Serverless
    │                              streaming
    │                                    │
    │                                    ▼
    │                           HTML reports / Dashboards
```

---

## Metric Standards

All reports must use these metric definitions. Any new metric must be added here before it ships.

| Metric | Formula | Grain | Notes |
|---|---|---|---|
| Watch Time | `SUM(watch_duration_seconds)` | per (entity, day) | entity = title / customer / device |
| Completion Rate | `COUNT(was_completed = true) / COUNT(*)` | per (entity, day) | session-level boolean |
| Session Length p50 / p95 | percentiles of `watch_duration_seconds` | per (entity, day) | use `PERCENTILE_APPROX` |
| DAU / WAU / MAU | distinct `customer_id` over 1d / 7d / 30d | global, also per geography | rolling windows |
| Title Reach | distinct `customer_id` per title | per (title, period) | per day, per week |
| Genre Mix | `watch_seconds` split by `dim_genre` | per period | normalised to 100% |
| Drop-off Distribution | histogram of `position_ms / runtime_ms` at session exit | per title | 10-bucket histogram |
| Cohort Retention | `% of cohort active in week N after signup` | per (signup_week, retention_week) | weekly grain |

Ratios and percentiles are computed in the aggregate layer — never inside an HTML report or a dashboard SQL.

---

## Pre-Aggregate Tables (Iceberg)

All five tables live under `glue_catalog.streaming_reporting.<name>` and are partitioned for predicate pushdown.

### content_engagement_daily — per title per day

```sql
CREATE TABLE glue_catalog.streaming_reporting.content_engagement_daily (
    engagement_date DATE,
    title_key BIGINT,
    tconst STRING,
    primary_title STRING,
    title_type STRING,
    genres STRING,
    distinct_viewers INT,
    sessions_count INT,
    total_watch_seconds BIGINT,
    completion_count INT,
    completion_rate DECIMAL(4,3),
    avg_session_seconds INT,
    p50_session_seconds INT,
    p95_session_seconds INT,
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/reporting/streaming/content_engagement_daily/'
PARTITIONED BY (engagement_date)
TBLPROPERTIES ('format-version' = '2');
```

### title_completion_funnel — per title, drop-off curve

10 position buckets (0-10%, 10-20%, ... 90-100%). Each row is one (title, bucket) pair with the count of sessions that reached that bucket and the drop-off rate vs the previous bucket.

```sql
CREATE TABLE glue_catalog.streaming_reporting.title_completion_funnel (
    funnel_date DATE,
    title_key BIGINT,
    tconst STRING,
    primary_title STRING,
    bucket_index INT,           -- 0..9
    bucket_low_pct DECIMAL(4,3),
    bucket_high_pct DECIMAL(4,3),
    sessions_reached INT,
    drop_off_rate DECIMAL(4,3), -- vs previous bucket
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/reporting/streaming/title_completion_funnel/'
PARTITIONED BY (funnel_date)
TBLPROPERTIES ('format-version' = '2');
```

### device_engagement_daily — per device per day

```sql
CREATE TABLE glue_catalog.streaming_reporting.device_engagement_daily (
    engagement_date DATE,
    device_key BIGINT,
    device_version_key BIGINT,
    device_type STRING,
    platform STRING,
    sessions_count INT,
    distinct_customers INT,
    total_watch_seconds BIGINT,
    avg_bitrate_kbps INT,
    rebuffering_seconds BIGINT,
    completion_rate DECIMAL(4,3),
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/reporting/streaming/device_engagement_daily/'
PARTITIONED BY (engagement_date)
TBLPROPERTIES ('format-version' = '2');
```

### cohort_retention_weekly — per signup_week × retention_week

```sql
CREATE TABLE glue_catalog.streaming_reporting.cohort_retention_weekly (
    signup_week DATE,           -- Monday of cohort signup week
    retention_week DATE,        -- Monday of measurement week
    weeks_since_signup INT,
    cohort_size INT,
    active_customers INT,
    retention_rate DECIMAL(4,3),
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/reporting/streaming/cohort_retention_weekly/'
PARTITIONED BY (signup_week)
TBLPROPERTIES ('format-version' = '2');
```

### genre_mix_daily — per day, normalised by genre

```sql
CREATE TABLE glue_catalog.streaming_reporting.genre_mix_daily (
    engagement_date DATE,
    genre_key INT,
    genre_name STRING,
    total_watch_seconds BIGINT,
    sessions_count INT,
    distinct_viewers INT,
    share_of_watch_pct DECIMAL(5,4),  -- normalised to 1.0 across all genres for the day
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/reporting/streaming/genre_mix_daily/'
PARTITIONED BY (engagement_date)
TBLPROPERTIES ('format-version' = '2');
```

---

## Aggregation Job Spec

Each pre-aggregate has a daily Glue PySpark job. All jobs read processed-zone Iceberg via Glue catalog and write reporting-zone Iceberg with `INSERT OVERWRITE PARTITION` semantics so that re-runs are idempotent.

| Job name | Inputs | Output | Partition rebuilt | Cadence |
|---|---|---|---|---|
| BuildContentEngagementDaily | `fact_daily_engagement` + `dim_title` | `content_engagement_daily` | engagement_date | daily 02:30 UTC |
| BuildTitleCompletionFunnel | `fact_view_sessions` + `dim_title` | `title_completion_funnel` | last 7 days | daily 02:35 UTC |
| BuildDeviceEngagementDaily | `fact_view_sessions` + `dim_device` + `dim_device_version` | `device_engagement_daily` | engagement_date | daily 02:40 UTC |
| BuildCohortRetentionWeekly | `fact_view_sessions` + `dim_customer` | `cohort_retention_weekly` | signup_week (last 12 weeks) | weekly Mon 04:00 UTC |
| BuildGenreMixDaily | `fact_view_sessions` + `dim_title` + `dim_genre` | `genre_mix_daily` | engagement_date | daily 02:45 UTC |

### Example: BuildContentEngagementDaily (SparkSQL)

```sql
INSERT OVERWRITE glue_catalog.streaming_reporting.content_engagement_daily
PARTITION (engagement_date = DATE('{{partition_date}}'))
SELECT
    DATE('{{partition_date}}')                                      AS engagement_date,
    f.title_key,
    t.tconst,
    t.primary_title,
    t.title_type,
    t.genres,
    COUNT(DISTINCT f.customer_key)                                  AS distinct_viewers,
    SUM(f.sessions_count)                                           AS sessions_count,
    SUM(f.total_watch_seconds)                                      AS total_watch_seconds,
    SUM(CASE WHEN f.completion_pct >= 0.9 THEN 1 ELSE 0 END)        AS completion_count,
    AVG(CASE WHEN f.completion_pct >= 0.9 THEN 1.0 ELSE 0.0 END)    AS completion_rate,
    CAST(AVG(f.total_watch_seconds / NULLIF(f.sessions_count, 0)) AS INT) AS avg_session_seconds,
    CAST(PERCENTILE_APPROX(f.total_watch_seconds, 0.50) AS INT)     AS p50_session_seconds,
    CAST(PERCENTILE_APPROX(f.total_watch_seconds, 0.95) AS INT)     AS p95_session_seconds,
    CURRENT_TIMESTAMP                                               AS _built_at
FROM glue_catalog.streaming_processed.fact_daily_engagement f
JOIN glue_catalog.streaming_processed.dim_title t
  ON f.title_key = t.title_key
WHERE f.engagement_date = DATE('{{partition_date}}')
GROUP BY f.title_key, t.tconst, t.primary_title, t.title_type, t.genres;
```

The same `INSERT OVERWRITE PARTITION` shape applies to the other four jobs — only the source CTE and the GROUP BY change.

---

## Redshift Architecture

The reporting layer uses a hot/cold split: 7 days of native Redshift tables for low-latency dashboards, plus Spectrum external tables for unbounded history. End users always query a unified view that picks the right tier per row.

### Hot tier — COPY-loaded, last 7 days

```sql
CREATE TABLE reporting.content_engagement_daily_hot (
    engagement_date DATE NOT NULL,
    title_key BIGINT NOT NULL,
    tconst VARCHAR(20),
    primary_title VARCHAR(500),
    distinct_viewers INT,
    sessions_count INT,
    total_watch_seconds BIGINT,
    completion_rate DECIMAL(4,3)
)
DISTSTYLE KEY DISTKEY (title_key)
SORTKEY (engagement_date, title_key);

-- Daily COPY (runs after Glue aggregation finishes, ~02:50 UTC)
COPY reporting.content_engagement_daily_hot
FROM 's3://acme-dw-streaming-xs2026/reporting/streaming/content_engagement_daily/engagement_date=2026-05-02/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole'
FORMAT AS PARQUET;

-- Daily prune (drop rows older than 7 days)
DELETE FROM reporting.content_engagement_daily_hot
WHERE engagement_date < CURRENT_DATE - INTERVAL '7 days';
```

The same pattern applies to a `*_hot` table per pre-aggregate; DISTKEY and SORTKEY are tuned per query pattern (see compute selection table).

### Cold tier — Spectrum external schema

```sql
CREATE EXTERNAL SCHEMA reporting_external
FROM DATA CATALOG DATABASE 'streaming_reporting'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole';
```

Spectrum reads Iceberg via the Glue Data Catalog — no DDL required per table, the external schema picks up the live catalog.

### Unified view — what dashboards query

```sql
CREATE VIEW reporting.content_engagement_daily AS
SELECT * FROM reporting.content_engagement_daily_hot
UNION ALL
SELECT
    engagement_date, title_key, tconst, primary_title,
    distinct_viewers, sessions_count, total_watch_seconds, completion_rate
FROM reporting_external.content_engagement_daily
WHERE engagement_date < CURRENT_DATE - INTERVAL '7 days';
```

Reasoning: hot 7-day Redshift gives sub-second queries on the data 95% of dashboards actually hit; Spectrum is slower (5-30 s) but unbounded and free of COPY/storage overhead. The view hides the split from BI tools.

---

## Daily Reports

Reports are built on top of the aggregates and published to the LeastAction catalog as `html_report` items.

| Report name | Frequency | Source aggregate(s) | Audience |
|---|---|---|---|
| Top Titles Daily | daily 03:00 UTC | `content_engagement_daily` | Product, Content team |
| Genre Engagement Weekly | Mon 04:00 UTC | `genre_mix_daily` (last 7 days) | Marketing |
| Device Performance Daily | daily 03:30 UTC | `device_engagement_daily` | Engineering |
| Drop-off Analysis | daily 04:00 UTC | `title_completion_funnel` | Product |
| Cohort Retention Weekly | Mon 05:00 UTC | `cohort_retention_weekly` | Growth |

### Build pipeline (per report)

1. Athena (or Redshift) query produces a result set against the unified view.
2. A Python operator converts the result set to HTML with Plotly charts and sortable tables.
3. The HTML file is uploaded to `s3://acme-dw-streaming-xs2026/reporting/html_reports/<report_name>/yyyy=*/mm=*/dd=*/`.
4. The report is registered in the LeastAction catalog as an `html_report` item with build metadata (rows, runtime, size).
5. Chat users can ask "show me top titles report" and the catalog returns the latest build.

### HTML report standards

- Header: report name, date range, generated timestamp, source aggregate(s)
- Tables: sortable columns, top/bottom 5 rows highlighted
- Charts: bar for daily trend, pie for category splits, line for retention curves
- Footer: "Data freshness — aggregate built at <_built_at>, report at <generated_at>"
- All times displayed in UTC

---

## Quality Checks

Each aggregation job emits per-build quality checks back to the LeastAction catalog. A failure halts the downstream report build.

| Check | Threshold | Action |
|---|---|---|
| Aggregates reconcile to fact tables | within ±0.5% on `total_watch_seconds` | Halt + alert |
| Hot tier row count vs Spectrum (overlap day) | exact match | Investigate skew, do not auto-resolve |
| Report build success | 100% per scheduled job | Page on-call |
| Distinct viewer count > 0 (smoke test) | always > 0 | Indicates pipeline silently dropped data |
| Genre share sums to 1.0 (±0.01) | per `engagement_date` in `genre_mix_daily` | Halt build |
| Cohort retention rate ∈ [0, 1] | hard bounds | Halt build |
| COPY rows loaded > 0 | per `*_hot` table per day | Indicates partition path mismatch |

---

## Compute Selection

| Job | Compute | Why |
|---|---|---|
| Glue PySpark aggregation (5 jobs) | Glue 10 DPUs | Iceberg writes, dim+fact joins, percentile_approx |
| Redshift COPY hot 7d | Redshift Serverless | Native to Redshift, fastest path from S3 Parquet |
| Spectrum reads | Redshift Serverless + S3 | Compute via Redshift, data via S3 — no copy needed |
| Hot tier DELETE prune | Redshift Serverless | Cheap on a sorted 7-day window |
| HTML report generation | Athena + Python operator | Cheap, simple, no Spark cluster needed for kilobyte result sets |
| Weekly cohort job | Glue 10 DPUs | 12-week rebuild dominates, runs Mon 04:00 UTC off-peak |

Glue is preferred over EMR for the same reason as skill #05 — the workload is predictable and EMR spin-up dominates runtime at this volume.

---

## Edge Cases

| Case | Handling |
|---|---|
| Aggregate vs fact drift | If reconciliation fails (>0.5% off), check for late-arriving session updates that landed after aggregation ran. Re-run aggregation with a 48h lookback for the sessions table. |
| Hot tier prune lag | If COPY runs before DELETE, hot tier briefly has 8 days. Add `DELETE` as the **first** step of the daily ETL, not the last. |
| Spectrum + Iceberg schema evolution | When `dim_title` gets a new (additive) column, Spectrum picks it up automatically because it reads Iceberg metadata. No DDL change in Redshift. The unified view must continue to project only the columns Redshift hot-tier knows about — add the column to the view explicitly when the hot table is altered. |
| Empty days | If a day has no playback data (incident), aggregates produce zero rows. Reports must render "no data today" gracefully — never crash on an empty result set. |
| Time zones | All `engagement_date` and `signup_week` values are UTC. Display layer can convert. Never store local-time dates in the reporting zone. |
| Title removed from IMDb mid-period | `dim_title.is_active = false` but the row stays. Aggregates continue to join — the title still appears in historical reports. |
| Cohort backfill | New retention week added every Monday. The job rebuilds the last 12 signup_weeks each run so late-arriving sessions update prior cohorts. |
| Hot tier partition mismatch | If COPY path uses the wrong `engagement_date=` value, 0 rows are loaded. The "COPY rows loaded > 0" quality check catches this. |
| Spectrum cost spike | A dashboard accidentally does `SELECT * FROM reporting.content_engagement_daily WITHOUT a date filter`. Add a workload management rule to abort Spectrum scans > 100 GB. |

---

## LeastAction Catalog Integration

- Each pre-aggregate Iceberg table is a catalog item under `streaming/reporting/`.
- Each report is a catalog item of type `html_report` under `reports/streaming/`.
- Lineage:
  - `content_engagement_daily` parents `fact_daily_engagement`, `dim_title`
  - `title_completion_funnel` parents `fact_view_sessions`, `dim_title`
  - `device_engagement_daily` parents `fact_view_sessions`, `dim_device`, `dim_device_version`
  - `cohort_retention_weekly` parents `fact_view_sessions`, `dim_customer`
  - `genre_mix_daily` parents `fact_view_sessions`, `dim_title`, `dim_genre`
  - Each `html_report` parents the aggregate(s) it queries
- Per-build metadata written back: `rows_aggregated`, `runtime_seconds`, `dpu_seconds_used`, `output_html_size_bytes`, `snapshot_id`, `report_url`.
- Quality score gate: a failed reconciliation check on an aggregate halts every report that depends on it.

---

## Chat Queries Enabled

Once registered, users can ask the catalog chat:

- "Show me the top titles report from yesterday"
- "What was watch time for tt0944947 last week?"
- "Compare device engagement on iPhone vs Roku for May"
- "Generate a drop-off analysis for Stranger Things"
- "How is the most-watched genre changing month-over-month?"
- "What's the week-4 retention for the 2026-03-02 signup cohort?"
- "Which devices had the highest rebuffering yesterday?"

---

## Downstream

Reports are consumed by humans via chat and dashboards. There is no further pipeline downstream — this is the serving layer and the end of the `streaming` pipeline. Any new report must reuse the metric definitions in this skill; any new metric must be defined here first.
