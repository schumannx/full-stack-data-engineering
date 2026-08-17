# Skill: Raw Zone — Landing → Parquet Transformation

## Purpose
Describes the first ETL stage of the `streaming` pipeline: the landing → raw transformation. JSON.gz playback events from skill #03 become deduplicated, type-cast, schema-validated Parquet rows in the raw zone every 15 minutes. Daily customer/device JSONL.gz snapshots and monthly IMDb TSV.gz files (skill #01) get the same Parquet conversion on their own cadences. The raw zone is the **single source of truth** for all downstream layers — the processed zone (skill #05) and every reporting layer reads from `raw/`, never from `landing/`. Landing files are retained 30 days for replay only.

---

## Stage Diagram

```
                       skill #03 (landing)
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   playback_events     customer_profiles      device_registry
   (JSON.gz, 5min)     (JSONL.gz, daily)      (JSONL.gz, daily)
        │                     │                     │
        │  every 15min        │  daily 03:30 UTC    │  daily 03:30 UTC
        ▼                     ▼                     ▼
  AWSGlueJsonToParquet  AWSAthenaCTAS         AWSAthenaCTAS
   .operator             (overwrite)           (overwrite)
        │                     │                     │
        ▼                     ▼                     ▼
   raw/streaming/       raw/streaming/        raw/streaming/
   playback_events/     customer_profiles/    device_registry/
   (Parquet, snappy)    (Parquet, snappy)     (Parquet, snappy)


   skill #01 (imdb_base/) ──monthly──→ AWSAthenaTSVToParquet.operator
                                              │
                                              ▼
                                       raw/imdb/<table>/
                                       (Parquet, snappy)
```

Streaming path runs every 15 min; snapshot path runs daily; IMDb path runs monthly. All three write to the same raw zone.

---

## Source and Target Locations

Bucket: `s3://acme-dw-streaming-xs2026/`

### Inputs (landing)
```
s3://acme-dw-streaming-xs2026/landing/streaming/playback_events/yyyy=*/mm=*/dd=*/hh=*/*.json.gz
s3://acme-dw-streaming-xs2026/landing/streaming/customer_profiles/yyyy=*/mm=*/dd=*/customers_full.jsonl.gz
s3://acme-dw-streaming-xs2026/landing/streaming/device_registry/yyyy=*/mm=*/dd=*/devices_full.jsonl.gz
s3://acme-dw-streaming-xs2026/imdb_base/title_basics/yyyy=*/mm=*/title.basics.tsv.gz
s3://acme-dw-streaming-xs2026/imdb_base/title_ratings/yyyy=*/mm=*/title.ratings.tsv.gz
s3://acme-dw-streaming-xs2026/imdb_base/title_akas/yyyy=*/mm=*/title.akas.tsv.gz
```

### Outputs (raw)
```
s3://acme-dw-streaming-xs2026/raw/streaming/playback_events/event_date=*/event_hour=*/*.parquet
s3://acme-dw-streaming-xs2026/raw/streaming/customer_profiles/yyyy=*/mm=*/dd=*/*.parquet
s3://acme-dw-streaming-xs2026/raw/streaming/device_registry/yyyy=*/mm=*/dd=*/*.parquet
s3://acme-dw-streaming-xs2026/raw/imdb/title_basics/yyyy=*/mm=*/*.parquet
s3://acme-dw-streaming-xs2026/raw/imdb/title_ratings/yyyy=*/mm=*/*.parquet
s3://acme-dw-streaming-xs2026/raw/imdb/title_akas/yyyy=*/mm=*/*.parquet
```

Quarantine zone: `s3://acme-dw-streaming-xs2026/raw/streaming/playback_events_quarantine/event_date=*/event_hour=*/`

---

## File Format Conversions

| Source | Source format | Target format | Compression | Notes |
|---|---|---|---|---|
| playback_events | JSON.gz (line-delimited) | Parquet | snappy | Schema cast, dedup, sort, partition by event_date+event_hour |
| customer_profiles | JSONL.gz | Parquet | snappy | Daily overwrite, partition by yyyy/mm/dd |
| device_registry | JSONL.gz | Parquet | snappy | Daily overwrite, partition by yyyy/mm/dd |
| IMDb title_basics | TSV.gz | Parquet | snappy | One-time + monthly, `\N` → NULL |
| IMDb title_ratings | TSV.gz | Parquet | snappy | One-time + monthly, `\N` → NULL |
| IMDb title_akas | TSV.gz | Parquet | snappy | One-time + monthly, `\N` → NULL |

Target Parquet file size: ~128 MB per file (Glue `groupFiles=inPartition`, `groupSize=134217728`).

---

## Transformations Applied — playback_events

The events fact is the most complex transformation. Each 15-min micro-batch performs the following steps in order:

### 1. Read
Read all `.json.gz` files in the current input partition (`landing/streaming/playback_events/yyyy=YYYY/mm=MM/dd=DD/hh=HH/`) **plus 90-min lookback** across the prior 6 hourly partitions to catch late events that landed after their event_timestamp's natural batch had already run.

### 2. Schema cast
| Field | From | To |
|---|---|---|
| event_timestamp | string (ISO8601) | TIMESTAMP (UTC) |
| server_received_at | string (ISO8601) | TIMESTAMP (UTC) |
| position_ms | string/int | INT |
| bitrate_kbps | string/int | INT |
| schema_version | string/int | INT |
| event_id, session_id, customer_id, title_id, device_id, device_version_id, event_type, geo_country | string | string (TRIM whitespace) |

### 3. Dedup
```sql
ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY server_received_at DESC, event_id ASC
) = 1
```
Keep newest by `server_received_at`; tie-break alphabetically on `event_id` for determinism.

### 4. Quarantine routing
Write to `raw/streaming/playback_events_quarantine/` (not `raw/streaming/playback_events/`) any row matching:

| Quarantine reason | Rule |
|---|---|
| Unknown event_type | `event_type NOT IN ('play','pause','seek','resume','complete','exit')` |
| Schema version mismatch | `schema_version != 1` |
| Future client clock | `event_timestamp > server_received_at + INTERVAL '5' MINUTE` |
| Invalid title_id | `NOT regexp_like(title_id, '^tt[0-9]{7,10}$')` |
| Required field missing | any of event_id, session_id, customer_id, title_id, event_timestamp IS NULL |
| Stale late-arrival | `event_timestamp < CURRENT_TIMESTAMP - INTERVAL '24' HOUR` |

### 5. Sort
Sort surviving rows by `(session_id ASC, event_timestamp ASC)`. Required so downstream session reconstruction (skill #05) can rely on chronological order within each session and use simple lag/lead window functions.

### 6. Add columns
| Column | Type | Source |
|---|---|---|
| event_date | DATE | `CAST(event_timestamp AS DATE)` — partition key |
| event_hour | INT | `HOUR(event_timestamp)` — partition key |
| _landing_file | string | path of source `.json.gz` (Glue `input_file_name()`) |
| _loaded_at | TIMESTAMP | wall-clock time the row was written to raw |

### 7. Write
Parquet, snappy, partitioned by `event_date, event_hour`. Use Glue dynamic partition overwrite — only the partitions touched by this micro-batch are rewritten; older partitions are untouched.

---

## Transformations Applied — Snapshots

### customer_profiles, device_registry
1. Read the day's JSONL.gz file from `landing/streaming/<table>/yyyy=*/mm=*/dd=*/`
2. No dedup (snapshots are already deduplicated upstream)
3. Cast `created_at`/`updated_at` to TIMESTAMP, `signup_date` to DATE, `household_size` to INT, `is_deprecated` to BOOLEAN
4. Add `_landing_file`, `_loaded_at`
5. Write Parquet, partitioned by `yyyy/mm/dd` — full overwrite of the day's partition

### IMDb tables (title_basics, title_ratings, title_akas)
1. Read TSV.gz with header from `imdb_base/<table>/yyyy=*/mm=*/`
2. **Convert literal `\N` → SQL NULL** for every column (Athena CTAS: `IF(col = '\N', NULL, col)`)
3. Cast: `startYear`, `endYear`, `runtimeMinutes`, `numVotes`, `ordering` → INT; `averageRating` → DECIMAL(3,1); `isAdult`, `isOriginalTitle` → BOOLEAN
4. Trim whitespace on string fields
5. Write Parquet, partitioned by `yyyy/mm` — full overwrite of the month's partition

---

## Output Schema — `raw_playback_events`

| Column | Type | Source | Notes |
|---|---|---|---|
| event_id | string | landing | PK |
| session_id | string | landing | groups events into a session |
| customer_id | string | landing | FK → raw_customer_profiles |
| title_id | string | landing | FK → raw_imdb_title_basics |
| device_id | string | landing | FK → raw_device_registry |
| device_version_id | string | landing | FK → raw_device_registry |
| event_type | string | landing | enum: play/pause/seek/resume/complete/exit |
| event_timestamp | TIMESTAMP | landing (cast) | UTC |
| server_received_at | TIMESTAMP | landing (cast) | UTC |
| position_ms | INT | landing (cast) | playback position |
| bitrate_kbps | INT | landing (cast) | streaming bitrate |
| geo_country | string | landing | ISO 3166-1 alpha-2 |
| schema_version | INT | landing (cast) | currently 1 |
| event_date | DATE | derived | partition key |
| event_hour | INT | derived | partition key (0-23) |
| _landing_file | string | derived | source S3 key for lineage |
| _loaded_at | TIMESTAMP | derived | when row reached raw |

12 source fields + 4 derived = 16 columns total.

---

## Partitioning Strategy

| Table | Partition cols | Why |
|---|---|---|
| raw_playback_events | event_date + event_hour | Matches landing layout; enables hour-level pruning for downstream MERGE in skill #05 |
| raw_customer_profiles | yyyy / mm / dd | Daily snapshot — partition by load date so old snapshots remain queryable for slowly-changing-dimension reconstruction |
| raw_device_registry | yyyy / mm / dd | Same as above |
| raw_imdb_title_basics | yyyy / mm | Monthly snapshot |
| raw_imdb_title_ratings | yyyy / mm | Monthly snapshot |
| raw_imdb_title_akas | yyyy / mm | Monthly snapshot |

Why `event_date` instead of `yyyy/mm/dd` for events? Skill #05 reads via Athena/Spark and `event_date BETWEEN ...` is faster to prune than `yyyy='2026' AND mm='05' AND dd='01'`. We accept the tiny loss of human-readability.

---

## Compute Selection Matrix

| Table | Daily volume | Compute choice | Operator | Why |
|---|---|---|---|---|
| raw_playback_events | ~30–50 GB JSON.gz / batch ~3 GB | AWS Glue PySpark (G.1X, 4–10 DPU) | `AWSGlueJsonToParquet.operator` | Volume too high for Athena CTAS within 15-min budget; Spark window functions handle dedup + sort cheaply |
| raw_customer_profiles | ~80 MB | Athena CTAS | `AWSAthenaJsonToParquet.operator` | Tiny; CTAS finishes in seconds |
| raw_device_registry | ~2 MB | Athena CTAS | `AWSAthenaJsonToParquet.operator` | Trivial |
| raw_imdb_title_basics | ~200 MB monthly | Athena CTAS | `AWSAthenaTSVToParquet.operator` | Monthly cadence = ample time |
| raw_imdb_title_ratings | ~10 MB monthly | Athena CTAS | `AWSAthenaTSVToParquet.operator` | Trivial |
| raw_imdb_title_akas | ~250 MB monthly | Athena CTAS | `AWSAthenaTSVToParquet.operator` | Monthly cadence = ample time |

Operator naming follows Phase 1 convention: `<Engine><SourceFormat>To<TargetFormat>.operator`.

---

## Quality Checks (V1, per micro-batch)

| Check | Threshold | Action on fail |
|---|---|---|
| Input file count | > 0 | Skip + alert (no events landed in window) |
| Dedup ratio (rows_in / rows_out) | < 1.05 | Warn at 1.05, alert at 1.10 (Firehose replaying excessively) |
| Quarantine rate | < 1% | Warn at 1%, alert at 5% |
| Schema conformity (% events with all 12 source fields) | > 99.9% | Halt batch |
| event_lag p99 | < 5 min | Alert at 5 min, page on-call at 15 min |
| Row count vs same-hour-of-week 7-day avg | within ±50% | Investigate anomaly, do not halt |
| Output Parquet readable (Athena MSCK REPAIR + SELECT 1) | passes | Halt batch + alert |

V1 score is computed as a weighted sum across these checks; gate value is documented in the catalog item.

---

## Late Arrival Handling

- **90-min lookback per batch** — events landing in earlier partitions but still being written when the prior batch ran will be picked up.
- **Re-dedup on the lookback window** — events from prior batches may already exist in raw. Dedup keeps newest by `_loaded_at` (later load wins) so a re-processed event overwrites its prior raw copy via dynamic partition overwrite.
- **Stale late events** (`event_timestamp > 24h ago`) are routed to quarantine, consistent with skill #03's late-arrival policy.
- **Cross-partition sessions** — sessions can span hour partitions. Sort within `session_id` is preserved at write time, but full session stitching happens in skill #05.

---

## Edge Cases

| Case | Policy |
|---|---|
| Partial gzip files | Firehose can write incomplete `.gz` on failure. Glue reader catches gzip decode errors and routes the file to `raw/streaming/_corrupt/` + alert. Batch continues with remaining files. |
| Empty partitions | Stream paused, no files. Skip + log; do not raise alert unless 3 consecutive batches are empty. |
| Schema drift (new field) | Preserve unknown JSON fields under a `_extra_fields` map<string,string> column. Iceberg in skill #05 promotes recurring fields to first-class columns. |
| IMDb `\N` nulls | Convert to true SQL NULL during CTAS; never store the literal string `\N` in raw Parquet. |
| Duplicate (event_id, server_received_at) pairs | Tie-break alphabetically on `event_id` ASC for determinism so reprocessing produces identical raw output. |
| event_id NULL | Cannot dedup safely → quarantine immediately. |
| Mixed UTF-8 / Latin-1 in geo strings | Force UTF-8 decode; replace invalid bytes with `?` and tag row in `_extra_fields.encoding_warning`. |
| Glue job timeout (> 12 min) | Auto-retry once with doubled DPU; if still failing, halt and page on-call (15-min SLA at risk). |

---

## LeastAction Catalog Integration

- Each raw table registers as a catalog item under `streaming/raw/`:
  - `streaming/raw/playback_events`
  - `streaming/raw/customer_profiles`
  - `streaming/raw/device_registry`
  - `streaming/raw/imdb/title_basics`, `title_ratings`, `title_akas`
- Lineage: each raw item declares `parent` = the corresponding landing item from skill #03 (or imdb_base item from skill #01).
- Per-batch metadata write-back:

| Field | Description |
|---|---|
| rows_in | Rows read from landing (after lookback) |
| rows_out | Rows written to raw (after dedup + quarantine) |
| dedup_ratio | rows_in / rows_out |
| quarantine_count | Rows routed to quarantine zone |
| event_lag_p99 | p99 of `server_received_at - event_timestamp` |
| runtime_seconds | Wall-clock duration of the Glue/Athena job |
| input_file_count | Number of `.json.gz` files read |
| output_partition_count | Number of (event_date, event_hour) partitions written |

Quality score gate: V1 score `< 70` halts downstream stages (skill #05 will not run).

---

## Chat Queries Enabled

Once registered, users can ask:

- "Did the 14:30 raw batch run?"
- "What's the dedup ratio for raw_playback_events today?"
- "Show me the quarantine count for the last hour."
- "When was raw_imdb_title_basics last refreshed?"
- "How many events did we process in raw today?"
- "What's the event_lag p99 for the last 4 batches?"

---

## Downstream

The raw zone is consumed by skill #05 (processed zone, Iceberg dimensions and facts). Any change to the raw schema — new column, type change, partition change — must be coordinated with skill #05's MERGE patterns and dim/fact column lists. Adding a column is safe (Iceberg schema evolution); removing or renaming requires a coordinated migration.
