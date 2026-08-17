# Streaming Data Warehouse on AWS — Design Doc

**Status:** Draft v2
**Author:** schumannx
**Last updated:** 2026-05-30
**Scope:** End-to-end design for a streaming data warehouse running on AWS, ingested via self-managed Kafka, transformed with Glue PySpark on Iceberg, and orchestrated by self-hosted Apache Airflow (CeleryExecutor).

> **v2 note.** This revision swaps the ingest/compute/orchestration layers for portfolio skill-breadth: Kinesis Firehose → **self-managed Kafka (KRaft)**, Athena CTAS → **Glue PySpark + Iceberg-Spark**, and the custom **LeastAction** orchestrator → **Apache Airflow**. The Kimball data model (§3) is unchanged. See `PLAN_v2.md` at the repo root for the consolidated build plan.

---

## 1. Overview

### 1.1 Problem statement

We want a production-shaped data platform that ingests streaming playback events from devices, lands them on S3, conforms them through a multi-zone lake architecture, materialises a Kimball dimensional model, and serves business reports. The platform must run on AWS, be observable end-to-end, and be orchestrated by Apache Airflow so we own a standard, industry-recognised scheduling/dependency/backfill layer.

### 1.2 Goals

1. **Mimic Streaming data realistically** — incremental, device-emitted playback events at scale, joined to a real-world title catalog (IMDb).
2. **Multi-zone lake on S3** — landing → raw → processed → reporting, with each zone owning a single concern (event capture, schema conformity, dimensional modelling, pre-aggregation).
3. **Kimball dimensional model on Iceberg v2** — three fact grains (event / session / daily) over a shared set of conformed dimensions.
4. **Business-grade reports** — top titles, watch time by country/device, completion rate, DAU/WAU/MAU, engagement leaderboards.
5. **Orchestration via Apache Airflow** — schedule, sequence, retry, sensor-gate, and backfill the batch job graph; self-hosted with CeleryExecutor on Docker.
6. **End-to-end observability** — every batch writes back metadata (rows in/out, dedup ratio, quarantine rate, lag p99) to a catalog item that drives ops dashboards and chat queries.

### 1.3 Non-goals

- A real recommendation system (we generate the engagement data; we do not score it).
- Real-time per-user personalisation latency budgets (< 1 s).
- Multi-region active-active. Single region (us-west-2) is sufficient.
- A shared org-wide Airflow platform. This deployment is scoped to this project's job graph.

### 1.4 Success metrics

| Metric | Target |
|---|---|
| Landing → raw freshness | ≤ 15 min p95 |
| Raw → processed freshness | ≤ 1 hr p95 (fact_playback_events micro-batch) |
| Processed → reporting freshness | ≤ 24 hr (daily roll-up) |
| Pipeline cost | ≤ $X/day at 50 GB/day ingest (see §6.2) |
| Data quality score per zone | ≥ 70 / 100 (V1 score) — gates downstream |
| Airflow DAG-run success rate | ≥ 99.5% over rolling 7 days |

---

## 2. High-Level Design

### 2.1 Business context

Streaming's core data question is: **who watched what, on which device, where, and for how long — and did they finish?** Everything else (recommendations, content acquisition decisions, infrastructure capacity planning, retention modelling) is a consumer of that base fact. Our platform is a faithful mimic of that core: a device emits playback events incrementally as a user watches a title, the events flow through an event bus to S3, and downstream consumers see conformed dimensional tables.

### 2.2 Source data shape

A device streams events incrementally during a viewing session. Each event captures the user's playback state at a moment in time — not a rolled-up session summary. Example for a single user watching *Terminator*:

```
device_id  content_id  event_timestamp        position_ms  event_type
xxx        tt0088247   2026-05-20T20:00:00Z   0            play
xxx        tt0088247   2026-05-20T20:00:05Z   5000         heartbeat
xxx        tt0088247   2026-05-20T20:00:10Z   10000        heartbeat
...
xxx        tt0088247   2026-05-20T21:38:00Z   5700000      complete
```

Event types: `play`, `pause`, `seek`, `resume`, `complete`, `exit`.

This shape is the **only** thing landing infrastructure needs to know about. Sessions and daily roll-ups are derived downstream.

### 2.3 End-to-end pipeline

Five zones, six skills, one direction of data flow:

```
imdbws.com  ─monthly─▶  imdb_base/      (skill #01 — IMDb TSV master, via Lambda)
                            │
generator.py ─Kafka─▶  landing/         (skill #02 + #03 — synthetic events via
                            │            Kafka(KRaft)→systemd consumer, Iceberg)
                            ▼
                         raw/           (skill #04 — Iceberg, Glue PySpark,
                            │            dedup, schema cast, quarantine)
                            ▼
                       processed/       (skill #05 — Iceberg v2 dims + facts,
                            │            Kimball model, MERGE INTO)
                            ▼
                       reporting/       (skill #06 — pre-aggregates,
                                         Redshift Spectrum, HTML reports)
```

Each batch arrow (landing→raw onward) is an Airflow task; the Kafka→landing arrow is a continuous systemd consumer, **not** orchestrated by Airflow. Each zone is read-only to its consumers; nothing reads upstream of itself except for backfills.

### 2.4 S3 storage layout

```
s3://acme-dw-streaming-xs2026/
  imdb_base/        ← IMDb TSV master (~1 GB gzipped, monthly refresh)
  landing/          ← Kafka-consumer-written Iceberg (hour-partitioned, micro-batch)
  raw/              ← Iceberg, schema-cast, dedup applied
  processed/        ← Iceberg v2 — dims (unpartitioned) + facts (partitioned by event_date)
  reporting/        ← Iceberg pre-aggregates for Redshift Spectrum + HTML reports
```

Why these five zones rather than fewer:

| Zone | Why it exists separately |
|---|---|
| `imdb_base/` | External vendor source (IMDb). Refresh cadence differs from streaming events; isolation prevents accidental rebuilds. |
| `landing/` | First queryable copy of what the device bus produced, written as Iceberg by the Kafka consumer. **Replay/DR is served from Kafka topic retention (≥7d), not from landing** (landing is a managed, compacted Iceberg table, not byte-immutable). |
| `raw/` | Single source of truth for downstream layers. Iceberg, deduped, type-cast. Decouples downstream from raw event parsing cost. |
| `processed/` | Kimball model. Iceberg gives us MERGE, time travel, schema evolution. |
| `reporting/` | Query-time-optimised pre-aggregates. Reporting users don't pay processed-zone cost. |

> **Replay/DR & compaction (v2).** Because landing is now a mutable Iceberg table written by a streaming consumer, two things follow: (1) the **Kafka topic** (`retention.ms ≥ 7 days`) is the authoritative replay source for reprocessing or DR — not landing-S3; (2) a scheduled **Iceberg maintenance** job (`rewrite_data_files` + `expire_snapshots` + `rewrite_manifests`) runs against the landing and fact tables to defeat the small-file problem inherent to streaming appends. The consumer micro-batches (one Iceberg append per `max(rows=50k, secs=60)` flush) to keep file counts sane between compactions.

### 2.5 Data model at a glance

Kimball dimensional model: **8 dimensions + 3 fact tables at three grains**.

```
                    ┌───────────────┐
                    │   dim_title   │
                    └───────┬───────┘
                            │
        ┌───────────────────┼────────────────────┐
        │                   │                    │
┌───────▼────┐    ┌─────────▼─────────┐   ┌──────▼──────┐
│ dim_genre  │    │  FACT TABLES      │   │ dim_customer│
└────────────┘    │                   │   └─────────────┘
                  │  fact_playback_   │
┌────────────┐    │  events (event)   │   ┌─────────────┐
│ dim_date   │───▶│                   │◀──│ dim_device  │
└────────────┘    │  fact_view_       │   └──────┬──────┘
                  │  sessions         │          │
┌────────────┐    │  (session)        │   ┌──────▼───────────┐
│ dim_time_  │───▶│                   │◀──│ dim_device_      │
│ of_day     │    │  fact_daily_      │   │ version          │
└────────────┘    │  engagement (day) │   └──────────────────┘
                  └─────────┬─────────┘
                            │
                    ┌───────▼──────────┐
                    │  dim_geography   │
                    └──────────────────┘
```

Three fact grains, same business process, different audiences:

| Fact | Grain | Type | Audience |
|---|---|---|---|
| `fact_playback_events` | 1 row per atomic event | Transaction | Engineers, ML |
| `fact_view_sessions` | 1 row per continuous session | Accumulating snapshot | Product / UX |
| `fact_daily_engagement` | 1 row per (customer, title, day) | Periodic snapshot | Execs, BI |

> **Note on dimension count.** Original HLD notes listed 5 dimensions (title, customer, device, device_version, date). The implemented model in skill #05 adds 3 static lookup dims: `dim_genre`, `dim_time_of_day`, `dim_geography`. These were added because they're heavily filtered/grouped in the reporting layer and benefit from being conformed dimensions rather than inline strings.

### 2.6 Reports delivered

Built in the reporting zone (skill #06):

- Top 10 watched titles (daily / weekly / monthly)
- Most engaged users (rolling 7-day, 30-day)
- Watch time by country
- Watch time by device type
- Completion rate by content / by content_type / by genre
- Daily / weekly / monthly active users (DAU / WAU / MAU)
- Bitrate distribution by device platform
- Premiere-Friday lift (`is_premiere_friday` from `dim_date`)

Each report is a SQL query against the reporting-zone Iceberg tables, rendered to HTML via skill #06's report generator. Reports refresh nightly; ad-hoc queries hit Athena/Redshift Spectrum directly against the same tables.

### 2.7 Tech stack at a glance

| Layer | Choice | Why |
|---|---|---|
| Object storage | Amazon S3 | Standard for lake architectures; lifecycle policies; cheap |
| Event bus | **Self-managed Kafka (KRaft) on EC2** | Industry-standard log; topic retention is the replay source; no ZooKeeper |
| Landing writer | **EC2 Python consumer (systemd)** | Always-on micro-batch consumer → landing Iceberg |
| Table format | Apache Iceberg v2 | MERGE INTO, schema evolution, time travel, partition pruning |
| Catalog | AWS Glue Data Catalog | Shared metadata across Glue / Athena / Redshift |
| Batch compute | **AWS Glue PySpark (G.1X) + Iceberg-Spark** | Spark for all raw→processed→reporting transforms |
| Ad-hoc SQL | Amazon Athena | Serverless SQL for validation queries + ad-hoc |
| Warehouse for reports | Redshift Serverless (Spectrum) | External schema over Glue catalog; query Iceberg without copy |
| Orchestration | **Apache Airflow (self-host, CeleryExecutor, Docker)** | Standard scheduling/deps/retry/backfill; see §4 |
| IaC | Terraform | Multi-cloud-friendly; ecosystem coverage; see Appendix B |

---

## 3. LLD — Data

### 3.1 Kimball's four decisions

The four decisions fix the model. They are made in order and never re-ordered.

#### Decision 1 — Business process

**Content engagement / playback behaviour.** A subscriber starts a title, may pause/seek/resume, and either completes or abandons. Every measurement in this model expresses some aspect of that behaviour.

#### Decision 2 — Grain

Three fact tables at three grains, because the same process is consumed at three resolutions:

| Level | Granularity | Question it answers |
|---|---|---|
| Atomic | 1 row per playback event | *How* did they watch? (pauses, seeks, buffering) |
| Session | 1 row per continuous viewing | *What* did they watch and did they finish? |
| Daily | 1 row per user × title × day | *Did* they engage on this day? |

Grain is declared at the table level. Never mix grains inside a single fact.

#### Decision 3 — Dimensions

| Dim | SCD type | Source | Notes |
|---|---|---|---|
| `dim_title` | SCD1 | IMDb (basics + ratings + akas) | tconst as natural key |
| `dim_customer` | SCD1 | raw_customer_profiles | customer_id as natural key |
| `dim_device` | SCD1 | raw_device_registry | device_id as natural key |
| `dim_device_version` | SCD1 | raw_device_registry | child of dim_device |
| `dim_genre` | SCD1 | derived from IMDb genres | pipe-split, small static |
| `dim_date` | static | generated | date_key as `YYYYMMDD` int |
| `dim_time_of_day` | static | generated | hour 0–23 + day_of_week + is_weekend |
| `dim_geography` | static | ISO 3166 lookup | country code as natural key |

All dims are SCD Type 1 in V1. SCD2 is reserved for a follow-up if "title metadata as of viewing date" becomes a real reporting need.

#### Decision 4 — Facts (measures)

Only additive (or semi-additive) measures live in fact tables. Ratios like `avg_completion` are computed at query time downstream.

| Level | Measure | Notes |
|---|---|---|
| event | `watch_seconds` | derived from successive event_timestamps |
| event | `position_ms`, `bitrate_kbps` | raw playback state |
| event | `play_count`, `pause_count`, `seek_count` | rolled up downstream |
| event | `buffering_seconds` | from rebuffer events |
| session | `session_duration_seconds` | `end_ts − start_ts` |
| session | `watch_duration_seconds` | actually-watched seconds (excludes pause) |
| session | `completion_pct`, `was_completed`, `was_force_closed` | session outcome |
| daily | `distinct_titles_watched`, `total_watch_seconds`, `sessions_count` | additive across user × title × day |

### 3.2 Dimension table schemas

#### `dim_title`

| Field | Type | Sample |
|---|---|---|
| title_key (PK) | BIGINT | 1, 2, 3 |
| tconst (NK / IMDb id) | STRING | tt0050083 |
| primary_title | STRING | 12 Angry Men |
| title_type | STRING | movie, tvSeries |
| genres | STRING | Crime,Drama |
| runtime_minutes | INT | 96 |
| imdb_rating | DECIMAL(3,1) | 9.0 |
| num_votes | INT | 850000 |

#### `dim_customer`

| Field | Type | Sample |
|---|---|---|
| customer_key (PK) | BIGINT | 1, 2, 3 |
| customer_id (NK) | STRING | cust_000000 |
| email_hash | STRING | sha256 hex |
| signup_date | DATE | 2026-05-02 |
| country | STRING | DE, GB, MX |
| plan_tier | STRING | basic, standard, premium |
| age_band | STRING | 25-34, 35-49, 50+ |
| household_size | INT | 1, 5, 3 |

#### `dim_device`

| Field | Type | Sample |
|---|---|---|
| device_key (PK) | BIGINT | 1, 2, 3 |
| device_id (NK) | STRING | console_ps, laptop_chrome |
| device_type | STRING | console, laptop, tv, mobile, tablet |
| platform | STRING | playstation, xbox, web, ios, android |
| device_model | STRING | PS5, Xbox-Series-X |

#### `dim_device_version`

| Field | Type | Sample |
|---|---|---|
| device_version_key (PK) | BIGINT | 1, 2, 3 |
| device_version_id (NK) | STRING | console_ps_v0, console_ps_v1 |
| device_key (FK → dim_device) | BIGINT | 1 |
| os_version | STRING | 8.0, 10.0 |
| app_version | STRING | v8.5.2, v8.6.1 |
| is_deprecated | BOOLEAN | false |

#### `dim_date`

| Field | Type | Sample |
|---|---|---|
| date_key (PK) | INT | 20260502 |
| full_date | DATE | 2026-05-02 |
| year, quarter, month, day | INT | 2026, 2, 5, 2 |
| month_name, day_name | STRING | May, Saturday |
| week_of_year | INT | 18 |
| is_weekend | BOOLEAN | true |
| is_premiere_friday | BOOLEAN | false |
| season | STRING | Spring |

#### `dim_genre`, `dim_time_of_day`, `dim_geography`

Static lookup dims with small row counts (~20–250 rows each). Built once, refreshed only when source taxonomy changes.

### 3.3 Fact table schemas

#### `fact_playback_events` — transaction grain

| Field | Type | Sample |
|---|---|---|
| event_id (PK) | STRING (uuid) | de980dc9-… |
| session_id (DD) | STRING (uuid) | 06debd54-… |
| customer_key (FK) | BIGINT | 94 |
| title_key (FK) | BIGINT | 35 |
| device_key (FK) | BIGINT | 3 |
| device_version_key (FK) | BIGINT | 7 |
| date_key (FK) | INT | 20260502 |
| event_type | STRING | play, pause, seek, complete |
| event_timestamp | TIMESTAMP | 2026-05-02 20:00:08 |
| position_ms | INT | 0, 420000 |
| bitrate_kbps | INT | 4500 |
| geo_country (DD) | STRING | AU, GB, FR |
| event_date / event_hour | DATE / INT | 2026-05-02 / 20 |

`(DD)` = degenerate dimension — values carried inline on the fact, not promoted to a dimension table.

Partition strategy: `event_date` + `event_hour`. Matches landing layout; enables hour-level pruning.

#### `fact_view_sessions` — accumulating snapshot

| Field | Type | Sample |
|---|---|---|
| session_id (PK, DD) | STRING | 06debd54-… |
| customer_key (FK) | BIGINT | 94 |
| title_key (FK) | BIGINT | 35 |
| device_key (FK) | BIGINT | 3 |
| device_version_key (FK) | BIGINT | 7 |
| date_key (FK) | INT | 20260502 |
| session_start_ts / session_end_ts | TIMESTAMP | 20:00:08 / 21:38:17 |
| watch_duration_seconds | INT | 5889 |
| content_duration_seconds | INT | 5700 |
| completion_pct | DECIMAL(4,3) | 1.000, 0.428 |
| pause_count, seek_count | INT | 1, 0 |
| was_completed / was_force_closed | BOOLEAN | true / false |

Accumulating snapshot semantics: a session row is **updated** (via `MERGE INTO`) as new events arrive in subsequent micro-batches, up to the 24-hour late-arrival window.

#### `fact_daily_engagement` — periodic snapshot

| Field | Type | Sample |
|---|---|---|
| engagement_date | DATE | 2026-05-02 |
| date_key (FK, PK part) | INT | 20260502 |
| customer_key (FK, PK part) | BIGINT | 47 |
| title_key (FK, PK part) | BIGINT | 16 |
| sessions_count | INT | 1 |
| total_watch_seconds | INT | 10888 |
| completion_pct | DECIMAL(4,3) | 1.000 |

Periodic snapshot: row written once per (customer, title, day) after the day closes. Idempotent re-runs replace the day's partition.

### 3.4 Per-zone processing details

#### `imdb_base/` — Skill #01

- Source: `https://datasets.imdbws.com/` (gzipped TSV files: title.basics, title.ratings, title.akas, name.basics, title.crew, title.principals).
- Cadence: monthly refresh.
- Storage: `s3://acme-dw-streaming-xs2026/imdb_base/<table>/yyyy=*/mm=*/*.tsv.gz`
- Role: external vendor master. Never written to by our pipeline.

#### `landing/` — Skills #02 + #03

- **Skill #02** is the synthetic data generator (`streaming-generator/`): produces realistic playback events, customer profiles, and device registry records from the IMDb title catalog. In v2 it publishes events to the Kafka topic `streaming.playback_events` (launched ad-hoc on EC2 via SSM Run Command).
- **Skill #03** is the always-on **systemd Kafka consumer**: a single-instance micro-batch writer that flushes one Iceberg append per `max(rows=50k, secs=60)` into hour-partitioned landing. At-least-once (offsets committed only after the Iceberg commit); duplicates resolved by `event_id` dedup in raw.
- Path: `landing/streaming/playback_events/event_date=*/event_hour=*/` (Iceberg).
- Cadence: continuous micro-batch for events; daily JSONL.gz snapshots for customer_profiles and device_registry at 03:30 UTC.
- Replay/DR: served from **Kafka topic retention (≥7d)**, not from landing (see §2.4). Landing is compacted by the `iceberg_maintenance` job.

#### `raw/` — Skill #04

- Job: landing Iceberg → raw Iceberg, every 15 minutes.
- Compute: AWS Glue PySpark + Iceberg-Spark (G.1X, 4–10 DPU) for playback_events; Glue PySpark for snapshots and IMDb.
- Transformations (events path):
  1. Read current hour partition + 90-min lookback (catches late events).
  2. Schema cast: timestamps → TIMESTAMP UTC, numerics → INT/DECIMAL, strings trimmed.
  3. Dedup: `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY server_received_at DESC) = 1`.
  4. Quarantine: rows with unknown event_type, future client clocks, invalid title_id, missing required fields, or stale (>24h late) routed to `raw/streaming/playback_events_quarantine/`.
  5. Sort by `(session_id, event_timestamp)` so downstream session reconstruction is cheap.
  6. Add derived columns: `event_date`, `event_hour`, `_landing_file`, `_loaded_at`.
  7. Write Iceberg (Parquet+ZSTD data files), partitioned by `event_date, event_hour`, via dynamic partition overwrite / `MERGE`.
- Quality checks per micro-batch: input file count > 0, dedup ratio < 1.05 warn / < 1.10 alert, quarantine rate < 1% warn / < 5% alert, schema conformity > 99.9%, event_lag p99 < 5 min, row count vs same-hour-of-week ±50%.

#### `processed/` — Skill #05

- Format: Apache Iceberg v2 (delete files, position deletes, ACID).
- Catalog: AWS Glue Data Catalog (`glue_catalog.streaming_processed.<table>`).
- Compute: AWS Glue PySpark (Iceberg-Spark).
- Triggers:
  - Dim tables: daily 04:00 UTC.
  - `fact_playback_events`: every 15 min micro-batch.
  - `fact_view_sessions`: every 15 min, 24h late-update window via `MERGE INTO`.
  - `fact_daily_engagement`: daily 02:00 UTC for previous day.
- Why Iceberg v2: schema evolution (event payload drift), ACID writes (concurrent micro-batches), time travel (audit + reprocessing), partition pruning, `MERGE INTO` semantics for the accumulating-snapshot session fact.

#### `reporting/` — Skill #06

- Pre-aggregated Iceberg tables sized for Redshift Spectrum query cost.
- Examples:
  - `agg_content_daily(report_date, title_key, total_views, total_watch_time_sec, unique_users, completion_rate)`
  - `agg_user_daily(report_date, customer_key, total_watch_time_sec, sessions_count, distinct_titles_watched)`
  - `agg_country_daily(report_date, country_code, total_watch_time_sec, unique_users)`
- HTML report generator: queries these aggregates, renders to HTML reports stored at `reporting/reports/<date>/`.
- Refresh cadence: nightly, after `fact_daily_engagement` closes the previous day.

### 3.5 Skills mapping

| Skill | File | Owns |
|---|---|---|
| 01 | `01_source_imdb_dataset.md` | IMDb TSV download + S3 publish, monthly |
| 02 | `02_synthetic_data_generator.md` | Generator code (titles → realistic events) |
| 03 | `03_source_streaming_events.md` | Landing zone writer (Kafka consumer → Iceberg) |
| 04 | `04_raw_zone.md` | Landing → raw transformation (this is the hot path) |
| 05 | `05_processed_zone.md` | Kimball model on Iceberg v2 |
| 06 | `06_reporting_zone.md` | Pre-aggregates + HTML reports |

Each batch skill is an Airflow task (§4) that writes per-run metadata (rows in/out, dedup ratio, quarantine count, runtime, partition count) to the `run_metadata` table, keyed by `data_interval_start`. Lineage is expressed as DAG edges plus Airflow Assets — each downstream DAG schedules on the Asset its parent produces.

### 3.6 Reports & example SQL

Top 10 watched titles (last 7 days):

```sql
SELECT
  t.primary_title,
  SUM(d.total_watch_seconds) / 3600.0 AS watch_hours,
  COUNT(DISTINCT d.customer_key) AS unique_viewers
FROM streaming_processed.fact_daily_engagement d
JOIN streaming_processed.dim_title t USING (title_key)
WHERE d.engagement_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY t.primary_title
ORDER BY watch_hours DESC
LIMIT 10;
```

Completion rate by genre:

```sql
SELECT
  g.genre_name,
  AVG(s.completion_pct) AS avg_completion,
  COUNT(*) AS sessions
FROM streaming_processed.fact_view_sessions s
JOIN streaming_processed.dim_title t USING (title_key)
JOIN streaming_processed.dim_genre g
  ON g.genre_name = ANY(string_split(t.genres, ','))
WHERE s.session_start_ts >= CURRENT_DATE - INTERVAL '30' DAY
  AND s.was_completed IS NOT NULL
GROUP BY g.genre_name
ORDER BY avg_completion DESC;
```

DAU / WAU / MAU:

```sql
SELECT
  CURRENT_DATE AS as_of,
  COUNT(DISTINCT CASE WHEN engagement_date = CURRENT_DATE - 1 THEN customer_key END) AS dau,
  COUNT(DISTINCT CASE WHEN engagement_date >= CURRENT_DATE - 7 THEN customer_key END) AS wau,
  COUNT(DISTINCT CASE WHEN engagement_date >= CURRENT_DATE - 30 THEN customer_key END) AS mau
FROM streaming_processed.fact_daily_engagement;
```

---

## 4. LLD — Orchestration (Apache Airflow)

### 4.1 Why Airflow

Airflow is the de-facto standard for batch data-pipeline orchestration, which makes it the right choice for a portfolio that needs recognisable breadth. It gives us, out of the box, what a custom tool would have to reimplement: cron + dependency scheduling, retries with backoff, SLA-miss detection, sensor-based gating, range backfill (`catchup`), data-aware scheduling (Assets), and a UI/REST API for run status.

**Scope boundary (important):** Airflow orchestrates the **batch** graph only — everything from `landing → raw` onward. It does **not** run the Kafka brokers or the always-on landing consumer; those are continuous processes managed by systemd on EC2. Airflow schedules things that *start, finish, and have dependencies*; a never-ending stream is not one of them.

### 4.2 Deployment — self-host, CeleryExecutor, on Docker

The same containerised Airflow image runs in both environments; only the container orchestrator and the stateful backends differ.

| | Dev (built & run for real) | Prod (committed, apply-on-demand, not left running) |
|---|---|---|
| Runs containers | Docker Compose (one host) | ECS Fargate |
| Image | custom `airflow/Dockerfile` (+ `providers-amazon`) | same image → ECR |
| Broker | `redis` container | ElastiCache Redis |
| Metadata + Celery result backend | `postgres` container | RDS Postgres |
| Cost | $0 | ~$70–100/mo while applied → `terraform destroy` after demo |

CeleryExecutor topology (dev compose services): `scheduler · webserver(:8080) · triggerer · worker · redis · postgres · airflow-init · flower(:5555, optional)`.

```
   ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
   │ Webserver UI │        │  Scheduler   │        │  Triggerer   │
   │   (:8080)    │        │ (cron+deps)  │        │ (async sensors)
   └──────┬───────┘        └──────┬───────┘        └──────┬───────┘
          │  read/write           │  enqueue              │
          ▼                       ▼                       │
   ┌──────────────┐        ┌──────────────┐               │
   │  Postgres    │◀──────▶│    Redis     │◀──────────────┘
   │ (metadata +  │        │   (broker)   │
   │  result bkd) │        └──────┬───────┘
   └──────────────┘               │ Celery
                                  ▼
                          ┌──────────────┐   StartJobRun   ┌──────────────┐
                          │ Celery       │ ──────────────▶ │ Glue / Lambda│
                          │ worker(s)    │ ◀── poll ─────  │ Redshift     │
                          └──────────────┘                 └──────────────┘
```

Config: `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`, `AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0`, `AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://…`. Scale-out demo: `docker compose up --scale airflow-worker=3`.

**Queue routing:** `default` queue for pipeline tasks; a separate `maintenance` queue for Iceberg compaction (heavier, off-peak, independently scalable). Operators set `queue=` accordingly.

### 4.3 The DAGs

> **Superseded by the implementation — see README.md § Orchestration for the chains as
> built.** Two things changed: `dq_check` moved *ahead* of the fact jobs, so bad raw data
> fails the run before it can reach the dimensional model; and `redshift_refresh` was
> dropped (Free-Plan account — Athena serves the same Iceberg tables, so nothing in Airflow
> touches Redshift). Two DAGs have been added since: `reporting_marts_dbt` and `dq_scorecard`.

```python
# imdb_monthly  (@monthly)
lambda_mirror_imdb >> glue_imdb_to_raw                         # emits Asset: imdb_raw

# streaming_microbatch  ("*/15 * * * *", catchup=False)
wait_landing(S3KeySensor) >> glue_raw_events \
    >> glue_fact_playback_events \
    >> glue_fact_view_sessions(MERGE, 24h window) \
    >> dq_check >> write_run_metadata                          # emits Asset: raw_events

# daily_rollup  (schedule=[Asset(raw_events)] + "0 2 * * *")
glue_dims_refresh >> glue_fact_daily_engagement \
    >> glue_reporting_aggregates >> validate_etl(10 assertions) \
    >> [redshift_refresh, lambda_render_html] >> write_run_metadata

# iceberg_maintenance  (daily off-peak, queue="maintenance")
glue_compaction_landing >> glue_compaction_facts
```

`daily_rollup` schedules on the `raw_events` **Asset** (data-aware) rather than a blind cron, so it fires when fresh raw data actually exists.

### 4.4 Operators & connections

| Need | Operator (amazon provider) |
|---|---|
| Wait for landing partition | `S3KeySensor` (deferrable → triggerer) |
| Run a Glue PySpark job | `GlueJobOperator` + `GlueJobSensor` |
| Invoke Lambda (IMDb mirror, HTML render) | `LambdaInvokeFunctionOperator` |
| Redshift external-schema refresh / report SQL | `RedshiftDataOperator` |
| Ad-hoc / validation SQL | `AthenaOperator` |
| Reconciliation assertions | `PythonOperator` → shared `common/validation.py` |
| Cross-DAG trigger | Airflow **Assets/Datasets** |

Connections: `aws_default` (dev = read-only-mounted `~/.aws`; prod = ECS task IAM role, **no static keys**), `redshift_default`. Bucket names and DPU counts are Airflow **Variables**.

### 4.5 Idempotency, backfill, observability

**Idempotency / backfill:** every Glue task receives `data_interval_start` and keys its S3 partitions / Iceberg `MERGE` on it, so re-runs are deterministic (dynamic partition overwrite for appends; `MERGE INTO` for the session fact). `catchup=True` then gives clean range backfill over historical dates; a task killed mid-flight and retried produces identical output — no data loss.

**Observability (replaces the old LeastAction catalog):** each task writes per-run metadata (rows in/out, dedup ratio, quarantine %, lag p99, runtime, partition count) to a `run_metadata` table keyed by `data_interval_start`, queryable via Athena. Lineage is DAG edges + Assets. Alerting is via SLA-miss callbacks and `on_failure_callback` → SNS/Slack. The Airflow UI + REST API answer "did the 14:30 batch run?" directly, and Athena over `run_metadata` answers "what was the lag p99 for the 14:00 hour?" — together covering the chat-query use cases the LeastAction catalog was designed for.

---

## 5. LLD — Ops, Infra, Business

### 5.1 Ops

**Data quality checks (per zone, V1 score, gating downstream):**

| Zone | Check | Threshold | Action |
|---|---|---|---|
| landing | Row count vs same-hour-of-week | within ±50% | warn |
| landing | Consumer lag (Kafka offset lag) | < 60 s | alert at 5 min, page at 15 min |
| landing | Iceberg commit cadence (gap between appends) | < 5 min | alert (consumer stalled) |
| raw | Dedup ratio | < 1.05 | warn at 1.05, alert at 1.10 |
| raw | Quarantine rate | < 1% | warn at 1%, alert at 5% |
| raw | Schema conformity | > 99.9% | halt batch |
| raw | event_lag p99 | < 5 min | alert at 5 min, page at 15 min |
| processed | Dim FK orphan rate | < 0.1% | route to `_orphan_fk` quarantine |
| processed | Fact row count vs raw row count | within ±2% | alert |
| processed | Iceberg snapshot lag | < 30 min | alert |
| reporting | Pre-agg row count vs source fact count | matches expected ratio | alert |

V1 score: weighted sum across the zone's checks. Score < 70 halts downstream. Score 70–89 runs but flags. Score 90+ is green.

**SLAs (freshness):**

| Path | SLA target | Page-on-call threshold |
|---|---|---|
| device → landing | ≤ 5 min p99 | 15 min |
| landing → raw | ≤ 15 min p95 | 30 min |
| raw → fact_playback_events | ≤ 1 hr p95 | 2 hr |
| raw → fact_view_sessions | ≤ 1 hr p95 (within 24h late window) | 2 hr |
| previous day → fact_daily_engagement | ≤ 06:00 UTC | 09:00 UTC |
| previous day → reporting aggregates | ≤ 08:00 UTC | 10:00 UTC |

**Incident response:** every alert links to a runbook; the Airflow UI shows the failing task/DAG-run directly, and an Athena query over `run_metadata` answers "what's broken?" by reading the latest per-batch metadata.

### 5.2 Infra

**Cost ballpark** (at 50 GB/day ingest, ~30M events/day):

| Component | Driver | Estimated $/day |
|---|---|---|
| S3 storage (all zones, ~3 TB over 90 days lifecycle) | GB-month | $1.50 |
| S3 PUT/GET (consumer writes + reads) | per million requests | $0.50 |
| Kafka brokers (EC2, KRaft) | always-on instance(s) | $1.50 |
| Landing consumer (EC2, systemd) | always-on instance | $0.80 |
| Glue PySpark (raw + processed + reporting + compaction, ~7 DPU-hr/day) | DPU-hr | $3.10 |
| Athena (validation + ad-hoc) | TB scanned | $0.30 |
| Redshift Serverless (reporting reads, on-demand) | RPU-hr | $0.25 |
| **Total (data plane, always-on)** | | **~$8/day** |

**Airflow (orchestration) cost** is separate and mostly on-demand: dev is **$0** (local Docker Compose); prod (ECS Fargate scheduler/web/triggerer/workers + RDS Postgres + ElastiCache Redis) is **~$70–100/mo only while applied** — default state is `terraform destroy`d, spun up for demos. So steady-state run cost is the ~$8/day data plane above.

**Efficiency levers:**

- Lifecycle policies: raw 1 year → Glacier; processed 3 years hot; reporting 5 years. (Landing replay is served by Kafka retention, so landing-S3 needs no long replay window — only enough to bridge consumer restarts.)
- Iceberg target file size: set `write.target-file-size-bytes` ≈ 128 MB so streaming appends and Glue writes land reasonably sized files.
- Iceberg maintenance (the `iceberg_maintenance` DAG, §4.3): `rewrite_data_files` + `expire_snapshots` + `rewrite_manifests` daily on the landing table and `fact_playback_events` to defeat the streaming small-file problem.
- Spot DPUs for Glue when batch latency budget allows (skill #05 dims, daily roll-ups).

**Governance / security:**

- One IAM role per zone (`role-streaming-landing-writer`, `role-streaming-raw-writer`, etc.). Workers assume only the role they need.
- Bucket policy on `acme-dw-streaming-xs2026` denies public access; KMS-encrypted at rest; TLS in transit.
- Lake Formation grants column-level access (e.g., `email_hash` masked for non-data-platform roles).
- Audit: CloudTrail + S3 access logs to a separate bucket; retained 1 year.
- Trust policy on each role limits assumption to the specific service principal (`glue.amazonaws.com`, `lambda.amazonaws.com`, `ecs-tasks.amazonaws.com` for the Airflow task role) — see Appendix A.

**Retention policy:**

| Zone | Hot | Cold (Glacier) | Total |
|---|---|---|---|
| landing (Iceberg) | 7 days | — | 7 days (bridge only; replay served by Kafka) |
| Kafka topic retention | 7 days | — | 7 days (authoritative replay/DR source) |
| raw | 90 days | 9 months | 1 year |
| processed | 3 years | — | 3 years |
| reporting | 5 years | — | 5 years |
| audit logs | 1 year | — | 1 year |

### 5.3 Business

**Decisions this platform feeds:**

| Decision | Report it depends on | Owner |
|---|---|---|
| Content acquisition (renew/drop a title) | Top-N watched, completion rate by content | Content team |
| Premiere-window marketing | `is_premiere_friday` lift, DAU on premiere days | Marketing |
| Infrastructure capacity (CDN peering, encoder fleet) | Bitrate distribution, peak concurrent sessions | Infra |
| Retention / churn modelling | DAU/WAU/MAU, sessions/user/week | Growth |
| Device platform investment (build a new app?) | Watch time by device type, completion rate by platform | Product |

**Impact framing:** a 1% improvement in completion rate on the top-100 titles correlates (per industry baseline) with ~0.3% reduction in monthly churn. At a hypothetical 10M subscribers and $15 ARPU, that's $5.4M/year. This platform is the substrate that lets the analytics team measure and chase that delta.

---

## 6. Edge cases & validations

### 6.1 Source-data edge cases

| Case | Validation |
|---|---|
| Title exists, no stream after launch | Cross-join `dim_title` × `dim_date` after the title's release year; flag rows with 0 sessions in the first 30 days. |
| Title had streams in (n−1) but none in (n) | Window function over `fact_daily_engagement` per title; flag titles with >7-day gap after sustained engagement. |
| Customer with sessions but no `dim_customer` row | Orphan-FK check in processed zone routes to `_orphan_fk` quarantine. |
| Device emits events with timestamp drift > 5 min ahead of server clock | Quarantined at raw zone as "future client clock". |
| Session spanning multiple hour partitions | Sort by `(session_id, event_timestamp)` preserved at raw write; full stitching in skill #05's session accumulator. |
| Event arrives 24 h after its `event_timestamp` | Routed to quarantine as "stale late-arrival". Session fact stops accepting updates after 24 h late window. |
| IMDb `\N` null sentinels | Converted to true SQL NULL by the Glue PySpark imdb→raw job. |
| Schema drift (new event field) | Captured into `_extra_fields` map at raw; recurring fields promoted to first-class columns at next schema review. |

### 6.2 Data model expected-vs-result validation

For each fact table, a daily validation job confirms:

- **Row count:** `count(fact)` matches `count(raw)` ± dedup ratio.
- **FK integrity:** `count(fact LEFT JOIN dim WHERE dim.* IS NULL) / count(fact) < 0.001`.
- **Measure invariants:**
  - `fact_playback_events.position_ms >= 0`
  - `fact_view_sessions.session_end_ts >= session_start_ts`
  - `fact_view_sessions.watch_duration_seconds <= session_duration_seconds`
  - `fact_view_sessions.completion_pct BETWEEN 0 AND 1.001` (allow 0.1% rounding tolerance)
  - `fact_daily_engagement.total_watch_seconds = SUM of underlying session watch_duration`
- **Cardinality:** `count(distinct customer_key) in fact_daily_engagement` matches DAU report.

Failed validations write to a `validation_results` table and block reporting refresh.

---

## 7. Appendix

### A. AWS IAM concepts

**Principal** — the general "who can access this" concept. Can be an AWS user, AWS account, IAM role, or AWS service.

```json
"Principal": { "AWS": "arn:aws:iam::123456789012:root" }
```

**Service Principal** — specifically an AWS-managed service identity.

```json
"Principal": { "Service": "lambda.amazonaws.com" }
```

Without the right service principal in a trust policy, services cannot start, roles cannot be assumed, and permissions fail (often silently). A large fraction of AWS debugging is: *did the trust policy allow the correct service principal?*

| Thing | Purpose |
|---|---|
| Permission Policy | What the role **can do** (e.g., read S3, write logs) |
| Trust Policy | Who can **use** (assume) the role |
| Service Principal | The AWS service named in the trust policy |

Flow:

```
Service Principal
       ↓ (allowed by trust policy?)
Can assume role
       ↓
Gets permissions from the role's permission policy
       ↓
Calls AWS resources
```

### B. IaC comparison — Terraform vs CloudFormation vs CDK

Infrastructure as Code (IaC) means managing cloud infrastructure with code rather than the console.

| Feature | CloudFormation | CDK | Terraform |
|---|---|---|---|
| Created by | AWS | AWS | HashiCorp |
| Multi-cloud | No | No | Yes |
| Language | YAML / JSON | Python / TS / Java / C# / Go | HCL |
| Programming logic | Limited | Yes | Partial |
| AWS-native | Yes | Yes | No |
| State management | AWS-managed | AWS-managed | Local / remote `tfstate` |
| Best for | Pure-AWS enterprise | AWS dev teams | Multi-cloud / platform teams |

**Pipelines:**

```
CDK code
   ↓ synthesises
CloudFormation template
   ↓ deploys
AWS APIs → AWS Resources
```

```
Terraform HCL
   ↓
Terraform provider
   ↓
AWS / Azure / GCP / Databricks / Snowflake / GitHub APIs
   ↓
Cloud + SaaS resources
```

**Choice for this project: Terraform.** Multi-cloud-ready, broad ecosystem (Databricks, Snowflake, GitHub, Cloudflare providers), and aligns with platform-engineering direction. CloudFormation/CDK remain valid for AWS-only shops.

### C. Resource links

- **S3 bucket:** https://us-west-2.console.aws.amazon.com/s3/buckets/acme-dw-streaming-xs2026?region=us-west-2
- **IMDb dataset source:** https://datasets.imdbws.com/
- **Skill files:** `skills/aws/analytics-team/streaming/01_…06_*.md`
- **Generator code:** `streaming-generator/`
- **DataFrame drills (validation, transform):** `streaming-dataframe-drills/`

### D. Future work

- **SCD2 on `dim_title`** when "title metadata as of viewing date" becomes a real reporting need.
- **Airflow RBAC / auth backend** — wire the webserver to an SSO/LDAP auth backend and per-DAG role mapping before any multi-user use.
- **Cross-region replication** for DR. Single-region us-west-2 is V1.
- **Real-time reporting tier** (sub-minute) via Kinesis Data Analytics or Flink, if a use case justifies the cost.
- **Recommendation/personalisation pipeline** consuming `fact_playback_events` — outside this design's scope.
