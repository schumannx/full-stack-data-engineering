# Skill: Synthetic Playback Event Generator

## Purpose
This generator produces realistic Streaming playback events on top of the IMDb tconst master loaded in skill #01. Real Streaming viewing data is not publicly available, so we fabricate it to seed the `streaming` pipeline end-to-end. The generator emits to Kinesis Data Streams in production (`streaming-playback-events` → Firehose → `s3://acme-dw-streaming/landing/streaming/playback_events/`) and to local JSONL.gz files in dev (`./generator_out/`). All output is reproducible — same `--seed` plus same flags yields bit-identical events.

---

## Synthetic Universe Sizing

| Entity | Count | Notes |
|---|---|---|
| Customers | 1,000,000 | Generated once, snapshotted daily |
| Device models | 50 | iPhone, Android, Roku, Apple TV, smart TVs, web browsers, consoles |
| Device versions | 200 | App versions x OS versions per model |
| Titles | ~1.5M | All IMDb tconsts with `numVotes >= 1000` (filtered to popular content) |
| Events per peak hour | ~100,000 | Tunable via `--rate` flag (events per second) |
| Events per day (default) | ~50,000,000 | Approx — depends on time-of-day weights |

---

## User Behavior Models

Three viewer archetypes, assigned at customer creation:

| Archetype | % of population | Sessions/week | Session length | Behavior |
|---|---|---|---|---|
| Heavy | 15% | Poisson(λ=20) | log-normal(μ=2.5, σ=0.6) hours | Prefer series, binge multiple episodes, prime-time + late-night |
| Medium | 60% | Poisson(λ=8) | log-normal(μ=2.0, σ=0.5) hours | Mix of movies and series, mostly evenings |
| Light | 25% | Poisson(λ=2) | log-normal(μ=1.5, σ=0.4) hours | Mostly movies on weekends, occasional |

---

## Genre Affinity Model

- Each customer is assigned a **Dirichlet-sampled** distribution over IMDb genres at creation time (concentration α=0.5 → realistic preference clustering).
- Heavy archetype gets sharper preferences (α=0.3); light archetype gets flatter (α=0.8).
- Title selection per session: weighted random over `genre_affinity x imdb_rating x log(numVotes)`.

---

## Time-of-Day Distribution

Sinusoidal weights peaking at 20:00–22:00 local time (prime time):

| Hour range | Weight |
|---|---|
| 00–06 | 0.05 |
| 06–08 | 0.15 |
| 08–12 | 0.20 |
| 12–17 | 0.25 |
| 17–19 | 0.50 |
| 19–22 | 1.00 (peak) |
| 22–24 | 0.40 |

Weekday vs weekend multiplier: weekend +50% on hours 12–23.

---

## Device Mix Archetypes

Each customer assigned a device-mix archetype at creation:

| Archetype | % of population | Primary device | Secondary devices |
|---|---|---|---|
| TV-primary | 40% | Roku / Apple TV / Smart TV | Mobile (occasional) |
| Mobile-primary | 30% | iPhone / Android | Laptop (rare) |
| Laptop-primary | 20% | Web browser (Chrome/Safari/Firefox) | Mobile (occasional) |
| Multi-device | 10% | Switches between all | All |

---

## Session Behavior Rules

- Every session opens with a `play` event at `position_ms=0`.
- Sessions may have 0–N pause/resume cycles (Poisson λ=0.5 for movies, λ=1.5 for series episodes).
- Sessions may have 0–N seek events (Poisson λ=0.3 for movies, λ=0 for series).
- A session ends with **either** `complete` (if `completion_pct >= 0.9`) **or** `exit` (otherwise).
- Drop-off probability is a function of position: 5%/min in first 10 minutes (high early drop-off), 1%/min thereafter.
- Series binge logic: after a `complete` on an episode, 70% chance of starting the next episode within 5 minutes (back-to-back sessions).

---

## Event Schema (output)

| Field | Type | Notes |
|---|---|---|
| event_id | UUID | Unique per event |
| session_id | UUID | Stable across all events in one viewing session |
| customer_id | string | FK to dim_customer |
| title_id | string | IMDb tconst (FK to dim_title) |
| device_id | string | FK to dim_device |
| device_version_id | string | FK to dim_device_version |
| event_type | enum | play, pause, seek, resume, complete, exit |
| event_timestamp | ISO8601 | When the event occurred (event time, not server time) |
| position_ms | int | Playback position in milliseconds at the time of the event |
| bitrate_kbps | int | Streaming bitrate (varies by device + network) |
| geo_country | string | ISO 3166 alpha-2, derived from customer's country |
| schema_version | int | Currently 1 — bumped on breaking changes |

---

## Invocation

```bash
python generator.py \
  --start-ts 2026-05-01T00:00:00Z \
  --end-ts 2026-05-01T23:59:59Z \
  --rate 1000 \
  --imdb-titles s3://acme-dw-streaming/imdb_base/title_basics/yyyy=2026/mm=05/ \
  --customers-snapshot s3://acme-dw-streaming/imdb_base/customers/yyyy=2026/mm=05/ \
  --output-target kinesis \
  --kinesis-stream streaming-playback-events \
  --seed 42
```

| Flag | Meaning |
|---|---|
| `--start-ts` / `--end-ts` | Time window to generate events for (events get timestamps within this window) |
| `--rate` | Events per second (default 1000, peaks ~10000 at prime time) |
| `--imdb-titles` | S3 prefix for the IMDb title_basics partition produced by skill #01 |
| `--customers-snapshot` | S3 prefix for the persisted customer universe |
| `--output-target` | `kinesis` (prod), `firehose` (alt), or `file` (local dev) |
| `--kinesis-stream` | Target stream name when output-target is `kinesis` |
| `--seed` | RNG seed — same seed + same params = bit-identical output |

---

## Reproducibility & Backfill

- Setting `--seed N` makes the generator deterministic across customer assignment, session timing, title selection, and event jitter.
- Customer + device universes are seeded once and persisted; subsequent runs reuse them so `customer_id` mappings are stable.
- Backfill mode: pass historical `--start-ts` / `--end-ts` to backfill any time window for testing.
- Universe regeneration: pass `--regenerate-universe` to rebuild customer/device pools (rare — breaks existing `dim_customer`).

---

## Edge Cases / Quality Rules

| Rule | Behavior |
|---|---|
| Clock drift | `event_timestamp` may lag wall-clock by up to 30s to simulate device-side buffering |
| Late events | 1% of events emit with a deliberate 60–300s lag to test late-arrival handling |
| Duplicate events | 0.5% of events are intentionally duplicated to test dedup logic |
| Schema mismatches | 0.1% of events emit with unknown `event_type` to test the quarantine path |
| Position monotonicity | `position_ms` non-decreasing across `play → pause → resume`; can jump only on `seek`. Generator enforces this. |
| Session timeouts | If a session has no events for 30 min, generator force-emits an `exit` event with the last known position |

---

## Generator Output Locations

| Mode | Target |
|---|---|
| Production | Kinesis Stream `streaming-playback-events` (5 shards, on-demand fallback) |
| Dev / Backfill | S3 directly: `s3://acme-dw-streaming/generator_replay/yyyy=*/mm=*/dd=*/hh=*/` |
| Local | `./generator_out/events_YYYYMMDD_HH.jsonl.gz` |

---

## LeastAction Catalog Integration

- The generator script is registered as a catalog item: `operator.python` named `StreamingSyntheticEventGenerator.operator`, located in folder `streaming/operators/`.
- Daily runs are scheduled as `task` items, each with a `payload` specifying the `--start-ts` / `--end-ts` for that day.
- Each generator run writes metadata back to the catalog:

| Metadata field | Description |
|---|---|
| events_emitted_count | Total events written to the output target |
| customers_active | Distinct `customer_id` count for the run |
| runtime_seconds | Wall-clock duration of the generator run |
| seed_used | RNG seed for this run (used for replay / debugging) |

---

## Chat Queries Enabled

Once this skill is registered, users can ask the LeastAction chat:

- "How many events were generated yesterday?"
- "What seed was used for the 2026-05-01 backfill?"
- "Generate a backfill for the 2026-04-15 day range."
- "Show the top 10 most-watched titles from yesterday's synthetic events."

---

## Downstream

The events emitted here are consumed by skill #03 (Kinesis Firehose → S3 landing) and modelled by the Iceberg ETL skills further down the pipeline. Any change to the event schema must bump `schema_version` and update both the Firehose landing skill and the staging Iceberg table.
