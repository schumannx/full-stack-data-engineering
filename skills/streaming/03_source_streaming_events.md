# Skill: Source Data — Streaming Playback Events on S3

## Purpose
Describes how playback events from the synthetic generator (skill #02) land in S3 via Kinesis Data Streams → Kinesis Firehose, and how daily customer/device snapshots arrive on the side. Used by downstream ETL skills (validation, raw transform, dimensional model) to know where events live, how they are partitioned, what schema to expect, what the event-lag SLA is, and what edge cases to handle.

---

## Ingestion Flow

```
Generator (skill #02) ──→ Kinesis Data Streams (5 shards, on-demand fallback)
                              │
                              ├──→ Real-time consumers (future)
                              │
                              └──→ Kinesis Firehose
                                       │ (buffer: 5 min OR 128 MB, whichever first)
                                       │ (dynamic partitioning ON)
                                       ↓
                                   S3 landing/streaming/playback_events/
                                       yyyy=YYYY/mm=MM/dd=DD/hh=HH/
                                          *.json.gz
```

Sibling daily snapshots take a separate path — generator writes JSONL.gz directly to S3 once per day at 03:00 UTC:

```
Generator ──→ S3 landing/streaming/customer_profiles/yyyy=YYYY/mm=MM/dd=DD/customers_full.jsonl.gz
Generator ──→ S3 landing/streaming/device_registry/yyyy=YYYY/mm=MM/dd=DD/devices_full.jsonl.gz
```

---

## Source Location

All landing data lives under one bucket, partitioned by event time (events) or snapshot date (dimensions):

```
s3://acme-dw-streaming/landing/streaming/
  playback_events/
    yyyy=2026/mm=05/dd=01/hh=14/
      events-1-2026-05-01-14-05-32.json.gz
      events-1-2026-05-01-14-10-58.json.gz
      ...
  customer_profiles/
    yyyy=2026/mm=05/dd=01/customers_full.jsonl.gz
  device_registry/
    yyyy=2026/mm=05/dd=01/devices_full.jsonl.gz
```

Pipeline: `streaming`. Bucket: `s3://acme-dw-streaming/`. Landing root: `landing/streaming/`.

---

## File Formats

| Source | Format | Compression | Encoding |
|---|---|---|---|
| playback_events | JSON (one event per line) | gzip | UTF-8 |
| customer_profiles | JSONL (one customer per line) | gzip | UTF-8 |
| device_registry | JSONL (one device per line) | gzip | UTF-8 |

Firehose default: each S3 file is ~5 MB (when buffer time hits) up to ~128 MB (when buffer size hits). Typical hour partition: 12–60 files.

---

## Schemas

### playback_events
| Column | Type | Notes |
|---|---|---|
| event_id | string (UUID) | PK |
| session_id | string (UUID) | groups events into a viewing session |
| customer_id | string | FK → customer_profiles |
| title_id | string | IMDb tconst, FK → title_basics (skill #01) |
| device_id | string | FK → device_registry |
| device_version_id | string | FK → device_registry |
| event_type | enum | play, pause, seek, resume, complete, exit |
| event_timestamp | ISO8601 | client time when event occurred |
| server_received_at | ISO8601 | when Kinesis received the event |
| position_ms | int | playback position at event time |
| bitrate_kbps | int | streaming bitrate |
| geo_country | string | ISO 3166-1 alpha-2 |
| schema_version | int | currently 1 |

### customer_profiles
| Column | Type | Notes |
|---|---|---|
| customer_id | string | PK |
| email_hash | string | SHA-256, synthetic but realistic |
| signup_date | date | |
| country | string | ISO 3166 |
| plan_tier | string | basic, standard, premium |
| age_band | string | 13-17, 18-24, 25-34, 35-49, 50+ |
| household_size | int | 1–5 |
| created_at | ISO8601 | |
| updated_at | ISO8601 | |

### device_registry
| Column | Type | Notes |
|---|---|---|
| device_id | string | PK, synthetic device serial |
| device_version_id | string | links to specific firmware/app version |
| device_type | string | tv, mobile, laptop, console, tablet |
| platform | string | ios, android, roku, appletv, web, xbox, playstation |
| device_model | string | iPhone-15-Pro, Roku-Ultra-2024, etc. |
| os_version | string | |
| app_version | string | |
| is_deprecated | boolean | true if no longer supported |

---

## Update Cadence

| Source | Cadence | Type | Notes |
|---|---|---|---|
| playback_events | Continuous (Firehose buffer 5 min / 128 MB) | Append-only | New file every ~5 min per shard |
| customer_profiles | Daily 03:00 UTC | Full snapshot | Overwrite daily |
| device_registry | Daily 03:00 UTC | Full snapshot | Overwrite daily |

---

## File Sizing

| Source | Per file | Per hour | Per day | Per month |
|---|---|---|---|---|
| playback_events | 5–128 MB (gzip) | ~2–3 GB peak / ~500 MB off-peak | ~30–50 GB | ~1–1.5 TB |
| customer_profiles | ~80 MB (1M rows) | — | ~80 MB | ~2.4 GB |
| device_registry | ~2 MB (50K rows) | — | ~2 MB | ~60 MB |

Sizing drives compute selection in the ETL skill — micro-batch jobs read one hour partition at a time.

---

## Event Lag SLAs

Define **event_lag** = `server_received_at - event_timestamp`.

| Percentile | Target | Alert threshold |
|---|---|---|
| p50 | < 5 s | > 30 s |
| p95 | < 30 s | > 2 min |
| p99 | < 2 min | > 5 min |

p99 > 5 min indicates upstream backlog — page on-call. Computed per micro-batch and written back to the catalog.

---

## Late Arrival Policy

- **Up to 24 h late:** events are accepted into their original `event_timestamp` partition (back-dated). Required because session reconstruction depends on roughly chronological order, and devices may buffer events when offline.
- **More than 24 h late:** events are written to `landing/streaming/playback_events_late/` for manual review.
- **ETL implications:** each micro-batch processes its own hour partition plus a 90-min lookback window to catch late events. The sessions table holds a 24 h late-update window before being declared final.
- Full-snapshot tables (customer_profiles, device_registry) always overwrite — no lookback needed.

---

## Edge Cases

- **Duplicate events:** Firehose can replay up to 24 h on consumer failure; generator also intentionally injects 0.5% duplicates. Dedup on `event_id` is mandatory in raw zone.
- **Out-of-order events:** events within a session can arrive out of order. ETL must sort by `event_timestamp` within `session_id` before reconstructing.
- **Unknown event_type:** 0.1% of events carry an unrecognized `event_type` (test traffic). Quarantine, do not fail.
- **Future timestamps:** events with `event_timestamp > server_received_at + 5 min` are quarantined as bogus client clocks.
- **Missing FK:** events whose `title_id` is not in IMDb (e.g. tconst deleted between months). Quarantine, log to lineage.
- **Schema drift:** new fields may appear (e.g. `network_quality`). Use Iceberg schema evolution — add columns, never drop.
- **Session_id leakage:** sessions can stay open across hour partitions. ETL must merge events across partitions.
- **Empty hour partitions:** rare but possible (stream paused). Skip gracefully, do not fail the pipeline.
- **Partial files:** Firehose can write incomplete `.gz` files on failure. Validate gzip integrity in V1 validation.

---

## LeastAction Catalog Integration

- Each S3 zone (`playback_events`, `customer_profiles`, `device_registry`) registers as a catalog item under `streaming/landing/`.
- Lineage: raw zone items declare `parent` = the corresponding landing item.
- Metadata written back per micro-batch: `file_count`, `total_bytes`, `event_count`, `event_lag_p99`, `dedup_ratio`.
- Quality score gate: V1 quality `< 70` halts downstream ETL.

---

## Chat queries enabled

Once registered, users can ask the catalog chat questions like:

- "When did the last micro-batch land?"
- "What's the event_lag p99 for the 14:00 hour?"
- "How many events did we land yesterday?"
- "Did the 03:00 customer snapshot complete?"
