# Skill: Source Data — IMDb Master Dataset

## Purpose
Describes the shape, location, and behaviour of the IMDb public dataset that serves as the master content lookup for the `streaming` real-time analytics pipeline. Every synthetic playback event references an IMDb `tconst` as its `title_id`. Use this skill to determine how to read, partition, and refresh IMDb source data into the DW raw zone.

---

## Source Location

IMDb publishes gzipped TSV files publicly at `https://datasets.imdbws.com/`. We mirror them monthly into a dedicated zone of the streaming bucket:

```
s3://acme-dw-streaming/
  imdb_base/
    <table>/
      yyyy=<year>/mm=<month>/
        <filename>.tsv.gz
```

Example (May 2026 snapshot):
```
s3://acme-dw-streaming/imdb_base/title_basics/yyyy=2026/mm=05/title.basics.tsv.gz
s3://acme-dw-streaming/imdb_base/title_ratings/yyyy=2026/mm=05/title.ratings.tsv.gz
s3://acme-dw-streaming/imdb_base/title_akas/yyyy=2026/mm=05/title.akas.tsv.gz
s3://acme-dw-streaming/imdb_base/name_basics/yyyy=2026/mm=05/name.basics.tsv.gz
s3://acme-dw-streaming/imdb_base/title_principals/yyyy=2026/mm=05/title.principals.tsv.gz
```

Original public source URL pattern: `https://datasets.imdbws.com/<filename>.tsv.gz` (e.g. `https://datasets.imdbws.com/title.basics.tsv.gz`).

---

## File Formats

| Table | Format | Delimiter | Has Header | Encoding | Compression | Null Encoding |
|---|---|---|---|---|---|---|
| title_basics | TSV | `\t` | yes | UTF-8 | gzip | literal `\N` |
| title_ratings | TSV | `\t` | yes | UTF-8 | gzip | literal `\N` |
| title_akas | TSV | `\t` | yes | UTF-8 | gzip | literal `\N` |
| name_basics | TSV | `\t` | yes | UTF-8 | gzip | literal `\N` |
| title_principals | TSV | `\t` | yes | UTF-8 | gzip | literal `\N` |

ID format conventions:
- `tconst` — title identifier, literal `tt` prefix + zero-padded number, e.g. `tt0111161`
- `nconst` — name (person) identifier, literal `nm` prefix, e.g. `nm0000001`

---

## Schemas

### title.basics
| Column | Type | Notes |
|---|---|---|
| tconst | string | PK, format `tt0000000` |
| titleType | string | movie, tvSeries, tvEpisode, short, etc. |
| primaryTitle | string | display title |
| originalTitle | string | original-language title (Unicode) |
| isAdult | boolean | `0` / `1` |
| startYear | int | YYYY, `\N` if unknown |
| endYear | int | YYYY, `\N` for non-series |
| runtimeMinutes | int | `\N` if unknown |
| genres | string | comma-separated, e.g. `Action,Drama` |

### title.ratings
| Column | Type | Notes |
|---|---|---|
| tconst | string | FK → title.basics |
| averageRating | decimal(3,1) | 1.0–10.0 |
| numVotes | int | total IMDb votes |

### title.akas
| Column | Type | Notes |
|---|---|---|
| titleId | string | FK → title.basics.tconst |
| ordering | int | sort order within a title |
| title | string | alternate title text |
| region | string | ISO 3166-1 alpha-2 |
| language | string | ISO 639 code |
| types | string | comma-separated (e.g. `dvd,festival`) |
| attributes | string | descriptive attributes |
| isOriginalTitle | boolean | `0` / `1` |

### name.basics
| Column | Type | Notes |
|---|---|---|
| nconst | string | PK, format `nm0000000` |
| primaryName | string | person's name |
| birthYear | int | YYYY, `\N` if unknown |
| deathYear | int | YYYY, `\N` if alive/unknown |
| primaryProfession | string | comma-separated professions |
| knownForTitles | string | comma-separated tconsts |

### title.principals
| Column | Type | Notes |
|---|---|---|
| tconst | string | FK → title.basics |
| ordering | int | sort order within a title |
| nconst | string | FK → name.basics |
| category | string | actor, director, writer, etc. |
| job | string | specific job title, `\N` if not applicable |
| characters | string | JSON-array of character names |

---

## Update Cadence

| Table | Cadence | Type | Notes |
|---|---|---|---|
| title_basics | Monthly, 1st @ 06:00 UTC | Full snapshot | Overwrite previous month's partition |
| title_ratings | Monthly, 1st @ 06:00 UTC | Full snapshot | Overwrite previous month's partition |
| title_akas | Monthly, 1st @ 06:00 UTC | Full snapshot | Overwrite previous month's partition |
| name_basics | Monthly, 1st @ 06:00 UTC | Full snapshot | Overwrite previous month's partition |
| title_principals | Monthly, 1st @ 06:00 UTC | Full snapshot | Overwrite previous month's partition |

IMDb publishes daily, but the catalog rarely changes meaningfully day-to-day — monthly is sufficient for streaming analytics.

---

## File Sizing

| Table | Approx Rows | Size (gzip) | Category |
|---|---|---|---|
| title_basics | ~9M | ~200 MB | medium |
| title_ratings | ~1.5M | ~10 MB | small |
| title_akas | ~30M | ~250 MB | medium |
| name_basics | ~13M | ~200 MB | medium |
| title_principals | ~80M | ~500 MB | large |

Sizing drives compute selection in the ETL skill — see `03_streaming_events_landing.md` and downstream load skills.

---

## Late Arrival Policy

- Not applicable — IMDb publishes a fresh complete snapshot at each download. Each monthly run replaces the prior partition.
- No lookback window required.
- Schema drift policy: rare but possible (IMDb has historically added new columns). Use Iceberg schema evolution — add columns, never drop.

---

## Edge Cases

| Case | Policy |
|---|---|
| `\N` nulls | Parse and convert to true SQL NULL in raw zone. Never store the literal string `\N`. |
| Genres delimiter | `title.basics.genres` uses **commas** (e.g. `Action,Drama`), not pipes. Split on `,` and emit `dim_genre` rows. |
| isAdult flag | Filter `isAdult=1` before joining to playback events for general analytics. Keep adult rows in raw zone for regulatory completeness. |
| Episodes need parents | `tvEpisode` rows reference a parent `tvSeries` via `title.episode.tsv.gz` (a 6th IMDb file). Not in initial scope — mention only. |
| Duplicate tconsts | IMDb guarantees PK uniqueness. Verify in V1 validation regardless. |
| Unicode in originalTitle | Korean, Japanese, Arabic, etc. must round-trip cleanly through TSV → Parquet → Iceberg. UTF-8 end-to-end. |
| Deleted/merged tconsts | A tconst can disappear between months. Never hard-delete from `dim_title`. Set `is_active=false` and write `last_seen_yyyymm` metadata. |
| Empty fields | Distinguish empty string `""` from `\N` (NULL); both can occur. |

---

## LeastAction Catalog Integration

| Aspect | Mapping |
|---|---|
| Folder | Each loaded table becomes a catalog item under `streaming/imdb/` |
| Items | `streaming/imdb/title_basics`, `streaming/imdb/title_ratings`, `streaming/imdb/title_akas`, `streaming/imdb/name_basics`, `streaming/imdb/title_principals` |
| Lineage | `dim_title` and `dim_genre` items have a parent relationship to `streaming/imdb/title_basics` raw item |
| Freshness | `last_refreshed_at` written back after each monthly load |
| Quality score | DQ checks (PK uniqueness, null-rate on `tconst`, row-count delta vs prior month) emit a quality score on each item |
| Pipeline tag | All items tagged with pipeline `streaming` |

---

## Chat Queries Enabled

Once this skill is registered, a LeastAction chat user can ask:

- "When was IMDb last refreshed?"
- "How many titles in `dim_title` are `tvSeries` vs `movie`?"
- "What genres exist in IMDb and how many titles per genre?"
- "Which IMDb tables feed the `streaming` pipeline and how big are they?"
