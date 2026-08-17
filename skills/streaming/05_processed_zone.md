# Skill: Processed Zone — Kimball Dimensional Model on Apache Iceberg v2

## Purpose
Defines the dimensional model for the `streaming` real-time analytics pipeline — 8 dimension tables and 3 fact tables, each materialised as an Apache Iceberg v2 table in the `processed/streaming/` zone. Applies Kimball's four dimensional modelling decisions explicitly (business process, grain, dimensions, facts), and lays out the MERGE/INSERT patterns that build these tables from the raw zone (skill #04). Iceberg v2 is the right format because we need schema evolution (event payload drift), ACID writes (concurrent micro-batches), time travel (audit + reprocessing), partition pruning (15-min batches against year-of-data), and `MERGE INTO` semantics for the accumulating-snapshot session fact. Output is consumed downstream by the reporting zone (skill #06).

---

## Shared Spec

| Item | Value |
|---|---|
| Bucket | `s3://acme-dw-streaming-xs2026/` |
| Processed zone path | `processed/streaming/<table_name>/` |
| Format | Apache Iceberg v2 (delete files, position deletes) |
| Catalog | AWS Glue Data Catalog (`glue_catalog.streaming_processed.<table>`) |
| Compute | AWS Glue PySpark (Iceberg-Spark); Athena for ad-hoc reads |
| Dim trigger | Daily 04:00 UTC |
| fact_playback_events trigger | Every 15 min (micro-batch) |
| fact_view_sessions trigger | Every 15 min, 24h late-update window |
| fact_daily_engagement trigger | Daily 02:00 UTC for previous day |

---

## Kimball's Four Decisions

The four decisions that fix the model. Make them in order; never re-order.

### Decision 1 — Business process

**Content engagement / playback behaviour.** A subscriber starts a title, may pause/seek/resume, and either completes or abandons. Every measurement in this model expresses some aspect of that behaviour.

### Decision 2 — Grain

We declare three fact tables at three different grains, because the same business process is consumed at three different resolutions by different audiences.

| Fact table | Grain | Type | Primary consumers |
|---|---|---|---|
| fact_playback_events | one row per atomic event (play / pause / seek / resume / complete / exit) | Transaction | Engineers, ML |
| fact_view_sessions | one row per continuous viewing session | Accumulating snapshot | Product / UX |
| fact_daily_engagement | one row per (customer, title, day) | Periodic snapshot | Execs, BI |

Grain is declared at table level — never mix grains inside a single fact.

### Decision 3 — Dimensions

| Dim | SCD type | Source | Notes |
|---|---|---|---|
| dim_title | SCD1 | IMDb (raw_imdb_title_basics + ratings + akas) | tconst as natural key |
| dim_customer | SCD1 | raw_customer_profiles | customer_id as natural key |
| dim_device | SCD1 | raw_device_registry | device_id as natural key |
| dim_device_version | SCD1 | raw_device_registry | device_version_id as natural key, child of dim_device |
| dim_genre | SCD1 | derived from IMDb genres pipe-split | small static |
| dim_date | static | generated | date_key as YYYYMMDD int |
| dim_time_of_day | static | generated | hour 0-23 + day_of_week + is_weekend |
| dim_geography | static | ISO 3166 lookup | country code as natural key |

All dims are SCD Type 1 in V1. SCD2 is reserved for a follow-up if "title metadata as of viewing date" becomes a real reporting need.

### Decision 4 — Facts (measures)

| Level | Measure | Notes |
|---|---|---|
| event | watch_seconds | derived from successive event_timestamps |
| event | completion_pct | position_ms / content_duration_ms |
| event | play_count | rolled up to session/day downstream |
| event | pause_count | same |
| event | seek_count | same |
| event | buffering_seconds | from rebuffer events |
| session | session_duration_seconds | end_ts - start_ts |
| session | watch_duration_seconds | actually-watched seconds (excludes pause) |
| session | was_completed | reached completion threshold |
| daily | distinct_titles_watched | count distinct title_key |
| daily | total_watch_seconds | sum |
| daily | sessions_count | distinct session_id |

Only additive (or semi-additive — completion_pct) measures live in fact tables. Ratios like avg_completion are computed downstream.

---

## Iceberg DDL — Dimension Tables

### dim_title

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_title (
    title_key BIGINT,
    tconst STRING,
    primary_title STRING,
    title_type STRING,
    genres STRING,
    runtime_minutes INT,
    imdb_rating DECIMAL(3,1),
    num_votes INT,
    is_active BOOLEAN,
    last_seen_yyyymm INT,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_title/'
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

### dim_customer

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_customer (
    customer_key BIGINT,
    customer_id STRING,
    email_hash STRING,
    signup_date DATE,
    country STRING,
    plan_tier STRING,
    age_band STRING,
    household_size INT,
    is_active BOOLEAN,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_customer/'
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

### dim_device

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_device (
    device_key BIGINT,
    device_id STRING,
    device_type STRING,
    platform STRING,
    device_model STRING,
    is_deprecated BOOLEAN,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_device/'
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

### dim_device_version

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_device_version (
    device_version_key BIGINT,
    device_version_id STRING,
    device_key BIGINT,
    os_version STRING,
    app_version STRING,
    released_at DATE,
    is_supported BOOLEAN,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_device_version/'
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

### dim_genre

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_genre (
    genre_key INT,
    genre_name STRING,
    parent_genre STRING,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_genre/'
TBLPROPERTIES ('format-version' = '2');
```

### dim_date (static, unpartitioned)

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_date (
    date_key INT,            -- YYYYMMDD
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    day_of_week INT,
    day_name STRING,
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_date/'
TBLPROPERTIES ('format-version' = '2');
```

### dim_time_of_day (static, unpartitioned)

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_time_of_day (
    time_of_day_key INT,     -- HH (0-23)
    hour INT,
    daypart STRING,          -- early_morning | morning | afternoon | evening | late_night
    is_prime_time BOOLEAN
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_time_of_day/'
TBLPROPERTIES ('format-version' = '2');
```

### dim_geography

```sql
CREATE TABLE glue_catalog.streaming_processed.dim_geography (
    geography_key INT,
    country_code STRING,     -- ISO 3166-1 alpha-2
    country_name STRING,
    region STRING,           -- AMER | EMEA | APAC | LATAM
    sub_region STRING,
    currency_code STRING,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/dim_geography/'
TBLPROPERTIES ('format-version' = '2');
```

---

## Iceberg DDL — Fact Tables

### fact_playback_events (transaction grain — append-only)

```sql
CREATE TABLE glue_catalog.streaming_processed.fact_playback_events (
    event_id STRING,
    session_id STRING,
    customer_key BIGINT,
    title_key BIGINT,
    device_key BIGINT,
    device_version_key BIGINT,
    date_key INT,
    time_of_day_key INT,
    geography_key INT,
    event_type STRING,
    event_timestamp TIMESTAMP,
    position_ms INT,
    bitrate_kbps INT,
    schema_version INT,
    _loaded_at TIMESTAMP,
    event_date DATE,
    event_hour INT
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/fact_playback_events/'
PARTITIONED BY (event_date, bucket(24, event_hour))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
```

Partitioning rationale: `event_date` gives us cheap day-level pruning for backfills; `bucket(24, event_hour)` keeps per-hour file groups balanced under heavy peak load without exploding the partition count.

### fact_view_sessions (accumulating snapshot)

```sql
CREATE TABLE glue_catalog.streaming_processed.fact_view_sessions (
    session_id STRING,
    customer_key BIGINT,
    title_key BIGINT,
    device_key BIGINT,
    device_version_key BIGINT,
    session_start_date DATE,
    session_start_ts TIMESTAMP,
    session_end_ts TIMESTAMP,
    watch_duration_seconds INT,
    content_duration_seconds INT,
    completion_pct DECIMAL(4,3),
    pause_count INT,
    seek_count INT,
    buffering_total_ms BIGINT,
    was_completed BOOLEAN,
    was_force_closed BOOLEAN,
    _last_event_at TIMESTAMP,
    _updated_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/fact_view_sessions/'
PARTITIONED BY (session_start_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

Accumulating-snapshot rationale: a session is updated repeatedly within a 24h window (more events keep arriving) — `merge-on-read` deletes are mandatory.

### fact_daily_engagement (periodic snapshot, full rebuild per day)

```sql
CREATE TABLE glue_catalog.streaming_processed.fact_daily_engagement (
    engagement_date DATE,
    customer_key BIGINT,
    title_key BIGINT,
    sessions_count INT,
    total_watch_seconds INT,
    completion_pct DECIMAL(4,3),
    last_position_ms INT,
    last_session_end_ts TIMESTAMP,
    _built_at TIMESTAMP
) USING iceberg
LOCATION 's3://acme-dw-streaming-xs2026/processed/streaming/fact_daily_engagement/'
PARTITIONED BY (engagement_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
);
```

---

## MERGE / INSERT Patterns

### SCD1 dim build — dim_title from IMDb

```sql
MERGE INTO glue_catalog.streaming_processed.dim_title t
USING (
    SELECT
        XXHASH64(b.tconst) AS title_key,
        b.tconst,
        b.primary_title,
        b.title_type,
        b.genres,
        b.runtime_minutes,
        r.average_rating AS imdb_rating,
        r.num_votes,
        TRUE AS is_active,
        CAST(DATE_FORMAT(CURRENT_DATE, 'yyyyMM') AS INT) AS last_seen_yyyymm,
        CURRENT_TIMESTAMP AS _updated_at
    FROM glue_catalog.streaming_raw.raw_imdb_title_basics b
    LEFT JOIN glue_catalog.streaming_raw.raw_imdb_title_ratings r
      ON b.tconst = r.tconst
) s
ON t.tconst = s.tconst
WHEN MATCHED THEN UPDATE SET
    primary_title     = s.primary_title,
    title_type        = s.title_type,
    genres            = s.genres,
    runtime_minutes   = s.runtime_minutes,
    imdb_rating       = s.imdb_rating,
    num_votes         = s.num_votes,
    is_active         = TRUE,
    last_seen_yyyymm  = s.last_seen_yyyymm,
    _updated_at       = s._updated_at
WHEN NOT MATCHED THEN INSERT (
    title_key, tconst, primary_title, title_type, genres,
    runtime_minutes, imdb_rating, num_votes, is_active,
    last_seen_yyyymm, _updated_at
) VALUES (
    s.title_key, s.tconst, s.primary_title, s.title_type, s.genres,
    s.runtime_minutes, s.imdb_rating, s.num_votes, s.is_active,
    s.last_seen_yyyymm, s._updated_at
);
```

The same pattern applies to dim_customer, dim_device, dim_device_version. dim_genre is rebuilt with a `CREATE OR REPLACE` from the pipe-split genres column.

### fact_playback_events — micro-batch append

```sql
INSERT INTO glue_catalog.streaming_processed.fact_playback_events
SELECT
    e.event_id,
    e.session_id,
    c.customer_key,
    t.title_key,
    d.device_key,
    dv.device_version_key,
    CAST(DATE_FORMAT(e.event_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    HOUR(e.event_timestamp)                                 AS time_of_day_key,
    g.geography_key,
    e.event_type,
    e.event_timestamp,
    e.position_ms,
    e.bitrate_kbps,
    e.schema_version,
    CURRENT_TIMESTAMP AS _loaded_at,
    DATE(e.event_timestamp) AS event_date,
    HOUR(e.event_timestamp) AS event_hour
FROM glue_catalog.streaming_raw.raw_playback_events e
LEFT JOIN glue_catalog.streaming_processed.dim_customer        c  ON e.customer_id        = c.customer_id
LEFT JOIN glue_catalog.streaming_processed.dim_title           t  ON e.title_id           = t.tconst
LEFT JOIN glue_catalog.streaming_processed.dim_device          d  ON e.device_id          = d.device_id
LEFT JOIN glue_catalog.streaming_processed.dim_device_version  dv ON e.device_version_id  = dv.device_version_id
LEFT JOIN glue_catalog.streaming_processed.dim_geography       g  ON e.geo_country        = g.country_code
WHERE e.event_date = DATE('{{partition_date}}')
  AND e.event_hour = {{partition_hour}};
```

Append-only — no MERGE. Dedup on `event_id` is performed in the raw zone (skill #04).

### fact_view_sessions — accumulating-snapshot MERGE

```sql
MERGE INTO glue_catalog.streaming_processed.fact_view_sessions s
USING (
    SELECT
        session_id,
        ANY_VALUE(customer_key)                                  AS customer_key,
        ANY_VALUE(title_key)                                     AS title_key,
        ANY_VALUE(device_key)                                    AS device_key,
        ANY_VALUE(device_version_key)                            AS device_version_key,
        DATE(MIN(event_timestamp))                               AS session_start_date,
        MIN(event_timestamp)                                     AS session_start_ts,
        MAX(event_timestamp)                                     AS session_end_ts,
        SUM(CASE WHEN event_type IN ('play','resume') THEN 1 ELSE 0 END) AS play_resume_count,
        SUM(CASE WHEN event_type = 'pause' THEN 1 ELSE 0 END)            AS pause_count,
        SUM(CASE WHEN event_type = 'seek'  THEN 1 ELSE 0 END)            AS seek_count,
        MAX(position_ms)                                         AS max_position_ms,
        MAX(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) = 1     AS was_completed,
        MAX(event_timestamp)                                     AS _last_event_at
    FROM glue_catalog.streaming_processed.fact_playback_events
    WHERE event_timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)
    GROUP BY session_id
) e
ON s.session_id = e.session_id
WHEN MATCHED THEN UPDATE SET
    session_end_ts          = e.session_end_ts,
    watch_duration_seconds  = CAST(UNIX_TIMESTAMP(e.session_end_ts) - UNIX_TIMESTAMP(e.session_start_ts) AS INT),
    pause_count             = e.pause_count,
    seek_count              = e.seek_count,
    was_completed           = e.was_completed,
    was_force_closed        = FALSE,
    _last_event_at          = e._last_event_at,
    _updated_at             = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    session_id, customer_key, title_key, device_key, device_version_key,
    session_start_date, session_start_ts, session_end_ts,
    watch_duration_seconds, pause_count, seek_count, was_completed,
    was_force_closed, _last_event_at, _updated_at
) VALUES (
    e.session_id, e.customer_key, e.title_key, e.device_key, e.device_version_key,
    e.session_start_date, e.session_start_ts, e.session_end_ts,
    CAST(UNIX_TIMESTAMP(e.session_end_ts) - UNIX_TIMESTAMP(e.session_start_ts) AS INT),
    e.pause_count, e.seek_count, e.was_completed,
    FALSE, e._last_event_at, CURRENT_TIMESTAMP
);
```

### fact_daily_engagement — full rebuild per day

```sql
INSERT OVERWRITE glue_catalog.streaming_processed.fact_daily_engagement
PARTITION (engagement_date = DATE('{{partition_date}}'))
SELECT
    DATE('{{partition_date}}')               AS engagement_date,
    customer_key,
    title_key,
    COUNT(DISTINCT session_id)               AS sessions_count,
    SUM(watch_duration_seconds)              AS total_watch_seconds,
    AVG(completion_pct)                      AS completion_pct,
    MAX_BY(content_duration_seconds, session_end_ts) AS last_position_ms,
    MAX(session_end_ts)                      AS last_session_end_ts,
    CURRENT_TIMESTAMP                        AS _built_at
FROM glue_catalog.streaming_processed.fact_view_sessions
WHERE session_start_date = DATE('{{partition_date}}')
   OR DATE(session_end_ts) = DATE('{{partition_date}}')
GROUP BY customer_key, title_key;
```

Periodic snapshot is always rebuilt for a single partition — no MERGE complexity.

---

## Surrogate Key Strategy

| Aspect | Choice | Reasoning |
|---|---|---|
| Type | `BIGINT` | Cheap join, fits in CPU register |
| Source | `XXHASH64(natural_key)` | Stable across re-runs; deterministic backfill |
| Alternative | sequence-based via Iceberg `IDENTITY` (future) | If natural keys ever leak into reports |
| Join semantics | dims join on `<dim>_key`, never on natural key | Hides natural-key churn (e.g. tconst rename) |
| Collision handling | Monitor: count rows where multiple natural keys map to one key | Synthetic data: zero. Real data: alert on >0 |

Hashed surrogate keys allow facts and dims to be built independently and re-attached on re-run — no need to coordinate sequence allocation.

---

## Iceberg-Specific Notes

| Topic | Rule |
|---|---|
| Schema evolution | ADD nullable columns only. Never DROP or RENAME — would break old snapshots and time-travel reads. |
| Time travel | Iceberg keeps snapshots. Default retention = 7 days via `history.expire.max-snapshot-age-ms = 604800000`. |
| Compaction | Weekly `CALL system.rewrite_data_files(table => 'fact_playback_events')` to merge small files (Firehose 5-min files create lots of <128MB parquet). |
| Snapshot expiry | Weekly `CALL system.expire_snapshots(table => '...', older_than => CURRENT_TIMESTAMP - INTERVAL 7 DAYS)`. |
| Orphan files | Monthly `CALL system.remove_orphan_files(table => '...')` — guards against failed writes leaving dangling parquet. |
| Partition evolution | Can change partition spec without rewriting old data. Useful if peak load forces day → hour bucketing. |
| Delete mode | `merge-on-read` for tables that update (sessions, daily). `copy-on-write` would force rewrite of every parquet on each MERGE — too expensive at session volume. |
| File sizing | `write.target-file-size-bytes = 134217728` (128 MB) for facts. Dims tolerate default. |

---

## Compute Selection

| Job | Compute | DPUs | Why |
|---|---|---|---|
| dims daily build (all 8) | Glue PySpark | 5 | Mostly small; one job covers all dims, parallelised inside Spark |
| fact_playback_events micro-batch | Glue PySpark | 10 | High volume, must finish inside 15-min budget |
| fact_view_sessions micro-batch | Glue PySpark | 10 | MERGE on 24h window — same scale as event fact |
| fact_daily_engagement | Athena CTAS / INSERT OVERWRITE | n/a | Cheap aggregation, runs once daily, no Spark cluster needed |
| Weekly compaction | Glue PySpark | 5 | Off-peak Sunday 03:00 UTC |

Glue is preferred over EMR because the workload is predictable (15-min cadence) and EMR cluster spin-up dominates runtime at this volume.

---

## Quality Checks (V3, per build)

| Check | Threshold | Where |
|---|---|---|
| dim FK coverage in fact_playback_events | > 99.9% | event title_id resolves to dim_title.tconst |
| dim FK coverage for customer_key, device_key | > 99.95% | nulls indicate dim build lag |
| Session reconstruction sanity | session_end_ts >= session_start_ts | always |
| Force-closed sessions | flag, do not fail | session > 24h with no `complete`/`exit` event |
| daily_engagement vs view_sessions reconciliation | within ±2% on SUM(watch_seconds) | catch double-counting |
| No duplicate (event_id, session_id) | 100% | unique constraint |
| Surrogate key collision | 0 | count distinct natural_key per surrogate_key |
| Snapshot lag | < 30 min | now() - max(_built_at) |

V3 writes pass/fail back to the LeastAction catalog; a failure halts skill #06.

---

## Edge Cases

| Case | Handling |
|---|---|
| Late events updating a closed session | Session was marked `was_completed=true`, then receives a new pause event 6h later. UPDATE the session, leave `was_completed=true` (the user did finish), set `was_force_closed=false`. The pause came after completion — typical "rewatching credits" behaviour. |
| Sessions spanning hour partitions | Events for the same `session_id` can land in two hour partitions. The 24h-lookback MERGE pattern handles this naturally — just make sure the lookback window is wider than the longest plausible session. |
| dim_title tconst removed from IMDb | Keep the row, set `is_active=false`, do not delete. Historical fact rows still join. `last_seen_yyyymm` shows the most recent IMDb snapshot it appeared in. |
| Identity collisions (surrogate key) | Hash on natural key → collisions impossible for synthetic data, ~1 in 10^15 for real. Monitor count of distinct natural keys per surrogate; alert if > 1. |
| NULL natural keys | Reject at raw zone. If anything slips through, hash NULL → constant — would collapse all NULL-keyed rows together. |
| Schema drift in events (new field) | Add nullable column to fact_playback_events via `ALTER TABLE ... ADD COLUMN`. Old snapshots remain readable. |
| Backfill of 30-day window | Run fact_playback_events micro-batch loop per hour; let MERGE on sessions absorb the late stream. fact_daily_engagement runs last per partition. |
| Glue job OOM during MERGE | Increase DPUs to 20; if persistent, switch to copy-on-write for that one MERGE. Almost always a skewed customer_id. |
| Time-travel read for audit | `SELECT * FROM fact_playback_events VERSION AS OF <snapshot_id>`. Use the snapshot id captured by the catalog at build time. |

---

## LeastAction Catalog Integration

- Each Iceberg table is a catalog item under `streaming/processed/`.
- Lineage:
  - `dim_title` parents `raw_imdb_title_basics`, `raw_imdb_title_ratings`
  - `dim_customer` parents `raw_customer_profiles`
  - `dim_device`, `dim_device_version` parent `raw_device_registry`
  - `fact_playback_events` parents `raw_playback_events` + all 8 dims
  - `fact_view_sessions` parents `fact_playback_events`
  - `fact_daily_engagement` parents `fact_view_sessions`
- Per-build metadata written back: `rows_inserted`, `rows_updated`, `rows_deleted`, `runtime_seconds`, `snapshot_id`, `dpu_seconds_used`, `bytes_written`.
- Quality score gate: V3 score `< 80` halts skill #06.

---

## Chat Queries Enabled

Once registered, users can ask the catalog chat:

- "Show me the top 10 titles by total_watch_seconds yesterday"
- "What's the completion rate for tt0903747 last week?"
- "How many sessions failed to complete in the last 24h?"
- "When was dim_title last updated?"
- "Time-travel: show fact_playback_events as of 2026-04-30"
- "Which dim has the highest FK miss rate this morning?"
- "How many DPUs did the 14:00 micro-batch consume?"

---

## Downstream

Skill #06 (reporting zone) reads from these Iceberg tables via Athena/Spark to build pre-aggregates that land in Redshift Spectrum for BI. The processed zone is the single source of truth — no other skill writes to its tables, and any new fact or dim follows the patterns above.
