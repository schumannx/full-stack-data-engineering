"""
Skill #05 — Processed Zone ETL (Iceberg dimensional model).

Builds the Kimball model:
  dim_title (from mock_tconsts inline)
  dim_customer (SCD1 from raw_customer_profiles)
  dim_device, dim_device_version (SCD1 from raw_device_registry)
  fact_playback_events (transaction grain — INSERT from raw)
  fact_view_sessions (accumulating snapshot — built from event aggregation)
  fact_daily_engagement (periodic snapshot — full rebuild from sessions)

Each table is Apache Iceberg v2 in S3, registered in Glue catalog.
Surrogate keys via ROW_NUMBER (rebuilt each run for validation simplicity).
"""

import sys
from pathlib import Path

from athena_runner import run_and_print
import config

# Reuse mock_tconsts from the generator project so we have a single source of truth
sys.path.insert(0, str(Path(__file__).parent.parent / "streaming-generator"))
from mock_tconsts import MOCK_TITLES  # noqa: E402


# ─── dim_title source: build VALUES from mock_tconsts ─────────────────────────

def build_titles_values_sql() -> str:
    """Inline 50 mock IMDb titles as a SELECT ... FROM (VALUES ...) source."""
    rows = []
    for t in MOCK_TITLES:
        title_escaped = t["primary_title"].replace("'", "''")
        rows.append(
            f"  ('{t['tconst']}', '{title_escaped}', '{t['title_type']}', "
            f"'{t['genres']}', {t['runtime_minutes']}, "
            f"DECIMAL '{t['imdb_rating']}', {t['num_votes']})"
        )
    values = ",\n".join(rows)
    return f"""
SELECT * FROM (VALUES
{values}
) AS t (tconst, primary_title, title_type, genres, runtime_minutes, imdb_rating, num_votes)
""".strip()


# ─── DDL: drop existing tables ────────────────────────────────────────────────

DROP_STATEMENTS = [
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.dim_title",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.dim_customer",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.dim_device",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.dim_device_version",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.dim_date",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.fact_playback_events",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.fact_view_sessions",
    f"DROP TABLE IF EXISTS {config.DB_PROCESSED}.fact_daily_engagement",
]


# ─── DDL: create Iceberg tables (CTAS pattern) ────────────────────────────────

def create_dim_title_sql() -> str:
    return f"""
CREATE TABLE {config.DB_PROCESSED}.dim_title
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}dim_title/',
  is_external = false
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY tconst) AS title_key,
  tconst,
  primary_title,
  title_type,
  genres,
  runtime_minutes,
  imdb_rating,
  num_votes,
  current_timestamp AS _updated_at
FROM (
{build_titles_values_sql()}
) src
"""


CREATE_DIM_CUSTOMER = f"""
CREATE TABLE {config.DB_PROCESSED}.dim_customer
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}dim_customer/',
  is_external = false
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
  customer_id,
  email_hash,
  signup_date,
  country,
  plan_tier,
  age_band,
  household_size,
  current_timestamp AS _updated_at
FROM {config.DB_RAW}.customer_profiles
"""


CREATE_DIM_DEVICE = f"""
CREATE TABLE {config.DB_PROCESSED}.dim_device
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}dim_device/',
  is_external = false
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY device_id) AS device_key,
  device_id,
  device_type,
  platform,
  device_model,
  current_timestamp AS _updated_at
FROM (
  SELECT DISTINCT device_id, device_type, platform, device_model
  FROM {config.DB_RAW}.device_registry
)
"""


CREATE_DIM_DATE = f"""
CREATE TABLE {config.DB_PROCESSED}.dim_date
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}dim_date/',
  is_external = false
) AS
SELECT
  CAST(date_format(d, '%Y%m%d') AS INTEGER) AS date_key,
  d AS full_date,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  date_format(d, '%M') AS month_name,
  day(d) AS day,
  day_of_week(d) AS day_of_week,
  date_format(d, '%W') AS day_name,
  week(d) AS week_of_year,
  day_of_week(d) IN (6, 7) AS is_weekend,
  day_of_week(d) = 5 AS is_premiere_friday,
  CASE
    WHEN month(d) IN (12, 1, 2) THEN 'Winter'
    WHEN month(d) IN (3, 4, 5)  THEN 'Spring'
    WHEN month(d) IN (6, 7, 8)  THEN 'Summer'
    ELSE 'Fall'
  END AS season,
  current_timestamp AS _updated_at
FROM UNNEST(sequence(DATE '2024-01-01', DATE '2027-12-31', INTERVAL '1' DAY)) AS t(d)
"""


CREATE_DIM_DEVICE_VERSION = f"""
CREATE TABLE {config.DB_PROCESSED}.dim_device_version
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}dim_device_version/',
  is_external = false
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY dv.device_version_id) AS device_version_key,
  dv.device_version_id,
  dv.device_id,
  d.device_key,
  dv.os_version,
  dv.app_version,
  dv.is_deprecated,
  current_timestamp AS _updated_at
FROM {config.DB_RAW}.device_registry dv
JOIN {config.DB_PROCESSED}.dim_device d ON dv.device_id = d.device_id
"""


CREATE_FACT_PLAYBACK_EVENTS = f"""
CREATE TABLE {config.DB_PROCESSED}.fact_playback_events
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}fact_playback_events/',
  partitioning = ARRAY['event_date'],
  is_external = false
) AS
SELECT
  e.event_id,
  e.session_id,
  c.customer_key,
  t.title_key,
  d.device_key,
  dv.device_version_key,
  CAST(date_format(e.event_date, '%Y%m%d') AS INTEGER) AS date_key,
  e.event_type,
  e.event_timestamp,
  e.position_ms,
  e.bitrate_kbps,
  e.geo_country,
  e.event_date,
  e.event_hour
FROM {config.DB_RAW}.playback_events e
LEFT JOIN {config.DB_PROCESSED}.dim_customer c ON e.customer_id = c.customer_id
LEFT JOIN {config.DB_PROCESSED}.dim_title t ON e.title_id = t.tconst
LEFT JOIN {config.DB_PROCESSED}.dim_device d ON e.device_id = d.device_id
LEFT JOIN {config.DB_PROCESSED}.dim_device_version dv ON e.device_version_id = dv.device_version_id
"""


# ─── fact_view_sessions: aggregate events into sessions ───────────────────────
# Logic:
# - First/last event timestamps define session start/end
# - Total watch_seconds = end_ts - start_ts (in real time)
# - completion_pct = max position / runtime
# - was_completed = last event is 'complete'
# - pause_count, seek_count = counts of those event types

CREATE_FACT_VIEW_SESSIONS = f"""
CREATE TABLE {config.DB_PROCESSED}.fact_view_sessions
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}fact_view_sessions/',
  partitioning = ARRAY['session_start_date'],
  is_external = false
) AS
WITH session_aggs AS (
  SELECT
    session_id,
    arbitrary(customer_key) AS customer_key,
    arbitrary(title_key) AS title_key,
    arbitrary(device_key) AS device_key,
    arbitrary(device_version_key) AS device_version_key,
    min(event_timestamp) AS session_start_ts,
    max(event_timestamp) AS session_end_ts,
    cast(date(min(event_timestamp)) AS date) AS session_start_date,
    max(position_ms) AS max_position_ms,
    sum(CASE WHEN event_type = 'pause' THEN 1 ELSE 0 END) AS pause_count,
    sum(CASE WHEN event_type = 'seek' THEN 1 ELSE 0 END) AS seek_count,
    max(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) = 1 AS was_completed,
    max(event_timestamp) AS _last_event_at
  FROM {config.DB_PROCESSED}.fact_playback_events
  GROUP BY session_id
)
SELECT
  s.session_id,
  s.customer_key,
  s.title_key,
  s.device_key,
  s.device_version_key,
  CAST(date_format(s.session_start_date, '%Y%m%d') AS INTEGER) AS date_key,
  s.session_start_ts,
  s.session_end_ts,
  date_diff('second', s.session_start_ts, s.session_end_ts) AS watch_duration_seconds,
  t.runtime_minutes * 60 AS content_duration_seconds,
  CAST(LEAST(s.max_position_ms / (t.runtime_minutes * 60.0 * 1000), 1.0) AS DECIMAL(4,3)) AS completion_pct,
  s.pause_count,
  s.seek_count,
  s.was_completed,
  false AS was_force_closed,
  s._last_event_at,
  current_timestamp AS _updated_at,
  s.session_start_date
FROM session_aggs s
JOIN {config.DB_PROCESSED}.dim_title t ON s.title_key = t.title_key
"""


CREATE_FACT_DAILY_ENGAGEMENT = f"""
CREATE TABLE {config.DB_PROCESSED}.fact_daily_engagement
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.PROCESSED_PATH}fact_daily_engagement/',
  partitioning = ARRAY['engagement_date'],
  is_external = false
) AS
SELECT
  session_start_date AS engagement_date,
  CAST(date_format(session_start_date, '%Y%m%d') AS INTEGER) AS date_key,
  customer_key,
  title_key,
  count(DISTINCT session_id) AS sessions_count,
  cast(sum(watch_duration_seconds) AS integer) AS total_watch_seconds,
  CAST(avg(completion_pct) AS DECIMAL(4,3)) AS completion_pct,
  cast(max(case when was_completed then content_duration_seconds * 1000 else watch_duration_seconds * 1000 end) AS integer) AS last_position_ms,
  max(session_end_ts) AS last_session_end_ts,
  current_timestamp AS _built_at
FROM {config.DB_PROCESSED}.fact_view_sessions
GROUP BY session_start_date, customer_key, title_key
"""


VERIFY_DATE_FK_COVERAGE = f"""
SELECT
  (SELECT count(*) FROM {config.DB_PROCESSED}.fact_playback_events) AS events_total,
  (SELECT count(*) FROM {config.DB_PROCESSED}.fact_playback_events e
     JOIN {config.DB_PROCESSED}.dim_date dd ON e.date_key = dd.date_key) AS events_joined,
  (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS sessions_total,
  (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions s
     JOIN {config.DB_PROCESSED}.dim_date dd ON s.date_key = dd.date_key) AS sessions_joined
"""


# ─── Verification ──────────────────────────────────────────────────────────────

VERIFY_QUERIES = [
    ("dim_title rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.dim_title"),
    ("dim_customer rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.dim_customer"),
    ("dim_device rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.dim_device"),
    ("dim_device_version rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.dim_device_version"),
    ("dim_date rows (2024-01-01..2027-12-31)", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.dim_date"),
    ("dim_date sample (today)", f"SELECT * FROM {config.DB_PROCESSED}.dim_date WHERE full_date = DATE '2026-05-02'"),
    ("fact_playback_events rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.fact_playback_events"),
    ("fact_view_sessions rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.fact_view_sessions"),
    ("fact_daily_engagement rows", f"SELECT count(*) AS n FROM {config.DB_PROCESSED}.fact_daily_engagement"),
    ("date_key FK coverage (events + sessions joined to dim_date)", VERIFY_DATE_FK_COVERAGE),
    (
        "events FK coverage (title_key non-null %)",
        f"""SELECT
          count(*) AS total,
          count(title_key) AS with_title,
          cast(count(title_key) * 100.0 / count(*) AS DECIMAL(5,2)) AS pct
        FROM {config.DB_PROCESSED}.fact_playback_events""",
    ),
    (
        "session reconciliation: events vs sessions count",
        f"""SELECT
          (SELECT count(DISTINCT session_id) FROM {config.DB_PROCESSED}.fact_playback_events) AS unique_sessions_in_events,
          (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS rows_in_sessions""",
    ),
    (
        "completion rate by title_type",
        f"""SELECT
          t.title_type,
          count(*) AS sessions,
          sum(case when s.was_completed then 1 else 0 end) AS completed,
          cast(sum(case when s.was_completed then 1.0 else 0.0 end) / count(*) AS DECIMAL(4,3)) AS completion_rate
        FROM {config.DB_PROCESSED}.fact_view_sessions s
        JOIN {config.DB_PROCESSED}.dim_title t ON s.title_key = t.title_key
        GROUP BY t.title_type""",
    ),
]


def main():
    print("=== Skill #05 — Processed Zone (Iceberg dim+fact) ===")
    print()

    print("Step 1: Drop existing tables")
    for sql in DROP_STATEMENTS:
        table = sql.split()[-1]
        run_and_print(sql, f"  drop {table}", print_results=False)

    print()
    print("Step 2: Build dimensions")
    run_and_print(create_dim_title_sql(), "  CTAS dim_title (50 mock tconsts)", print_results=False)
    run_and_print(CREATE_DIM_CUSTOMER, "  CTAS dim_customer", print_results=False)
    run_and_print(CREATE_DIM_DEVICE, "  CTAS dim_device", print_results=False)
    run_and_print(CREATE_DIM_DEVICE_VERSION, "  CTAS dim_device_version", print_results=False)
    run_and_print(CREATE_DIM_DATE, "  CTAS dim_date (2024-01-01..2027-12-31)", print_results=False)

    print()
    print("Step 3: Build facts (transaction grain first, then derived)")
    run_and_print(CREATE_FACT_PLAYBACK_EVENTS, "  CTAS fact_playback_events (transaction)", print_results=False)
    run_and_print(CREATE_FACT_VIEW_SESSIONS, "  CTAS fact_view_sessions (accumulating snapshot)", print_results=False)
    run_and_print(CREATE_FACT_DAILY_ENGAGEMENT, "  CTAS fact_daily_engagement (periodic snapshot)", print_results=False)

    print()
    print("Step 4: Verify")
    for label, sql in VERIFY_QUERIES:
        run_and_print(sql, f"  {label}")

    print()
    print("=== Skill #05 complete ===")


if __name__ == "__main__":
    main()
