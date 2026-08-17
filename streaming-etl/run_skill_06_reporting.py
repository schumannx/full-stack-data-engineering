"""
Skill #06 — Reporting Zone ETL (Iceberg pre-aggregates).

Builds the daily/weekly aggregates the reporting layer queries:
  content_engagement_daily   — per (title, day): viewers, sessions, watch time, completion
  device_engagement_daily    — per (device, day): watch time, sessions
  genre_mix_daily            — per (genre, day): watch time normalized
  title_completion_funnel    — per (title): position bucket histogram (drop-off curve)
  cohort_retention_weekly    — skipped (need multi-week data; we only have one day)

Each table is Iceberg in s3://.../reporting/streaming/<table>/.
Skips Redshift COPY/Spectrum tier — Athena queries the same Iceberg directly.
"""

from athena_runner import run_and_print
import config


DROP_STATEMENTS = [
    f"DROP TABLE IF EXISTS {config.DB_REPORTING}.content_engagement_daily",
    f"DROP TABLE IF EXISTS {config.DB_REPORTING}.device_engagement_daily",
    f"DROP TABLE IF EXISTS {config.DB_REPORTING}.genre_mix_daily",
    f"DROP TABLE IF EXISTS {config.DB_REPORTING}.title_completion_funnel",
]


CREATE_CONTENT_ENGAGEMENT_DAILY = f"""
CREATE TABLE {config.DB_REPORTING}.content_engagement_daily
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.REPORTING_PATH}content_engagement_daily/',
  partitioning = ARRAY['engagement_date'],
  is_external = false
) AS
SELECT
  s.session_start_date AS engagement_date,
  t.title_key,
  t.tconst,
  t.primary_title,
  t.title_type,
  t.genres,
  count(DISTINCT s.customer_key) AS distinct_viewers,
  count(*) AS sessions_count,
  cast(sum(s.watch_duration_seconds) AS bigint) AS total_watch_seconds,
  sum(case when s.was_completed then 1 else 0 end) AS completion_count,
  CAST(sum(case when s.was_completed then 1.0 else 0.0 end) / count(*) AS DECIMAL(4,3)) AS completion_rate,
  cast(avg(s.watch_duration_seconds) AS integer) AS avg_session_seconds,
  cast(approx_percentile(s.watch_duration_seconds, 0.50) AS integer) AS p50_session_seconds,
  cast(approx_percentile(s.watch_duration_seconds, 0.95) AS integer) AS p95_session_seconds,
  current_timestamp AS _built_at
FROM {config.DB_PROCESSED}.fact_view_sessions s
JOIN {config.DB_PROCESSED}.dim_title t ON s.title_key = t.title_key
GROUP BY s.session_start_date, t.title_key, t.tconst, t.primary_title, t.title_type, t.genres
"""


CREATE_DEVICE_ENGAGEMENT_DAILY = f"""
CREATE TABLE {config.DB_REPORTING}.device_engagement_daily
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.REPORTING_PATH}device_engagement_daily/',
  partitioning = ARRAY['engagement_date'],
  is_external = false
) AS
SELECT
  s.session_start_date AS engagement_date,
  d.device_type,
  d.platform,
  count(*) AS sessions_count,
  count(DISTINCT s.customer_key) AS distinct_viewers,
  cast(sum(s.watch_duration_seconds) AS bigint) AS total_watch_seconds,
  cast(avg(s.watch_duration_seconds) AS integer) AS avg_session_seconds,
  CAST(sum(case when s.was_completed then 1.0 else 0.0 end) / count(*) AS DECIMAL(4,3)) AS completion_rate,
  current_timestamp AS _built_at
FROM {config.DB_PROCESSED}.fact_view_sessions s
JOIN {config.DB_PROCESSED}.dim_device d ON s.device_key = d.device_key
GROUP BY s.session_start_date, d.device_type, d.platform
"""


CREATE_GENRE_MIX_DAILY = f"""
CREATE TABLE {config.DB_REPORTING}.genre_mix_daily
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.REPORTING_PATH}genre_mix_daily/',
  partitioning = ARRAY['engagement_date'],
  is_external = false
) AS
WITH genre_split AS (
  SELECT
    s.session_start_date AS engagement_date,
    trim(g) AS genre,
    s.watch_duration_seconds
  FROM {config.DB_PROCESSED}.fact_view_sessions s
  JOIN {config.DB_PROCESSED}.dim_title t ON s.title_key = t.title_key
  CROSS JOIN UNNEST(split(t.genres, ',')) AS u(g)
),
totals AS (
  SELECT engagement_date, sum(watch_duration_seconds) AS total
  FROM genre_split GROUP BY engagement_date
)
SELECT
  gs.engagement_date,
  gs.genre,
  cast(sum(gs.watch_duration_seconds) AS bigint) AS watch_seconds,
  CAST(sum(gs.watch_duration_seconds) * 100.0 / max(t.total) AS DECIMAL(5,2)) AS pct_of_day,
  current_timestamp AS _built_at
FROM genre_split gs
JOIN totals t ON gs.engagement_date = t.engagement_date
GROUP BY gs.engagement_date, gs.genre
"""


CREATE_TITLE_COMPLETION_FUNNEL = f"""
CREATE TABLE {config.DB_REPORTING}.title_completion_funnel
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  location = '{config.REPORTING_PATH}title_completion_funnel/',
  is_external = false
) AS
SELECT
  t.title_key,
  t.tconst,
  t.primary_title,
  t.title_type,
  -- 10 buckets: 0-9 percent, 10-19 percent, ... 90-100 percent
  cast(LEAST(CAST(s.completion_pct * 10 AS integer), 9) AS integer) AS bucket,
  count(*) AS sessions_in_bucket,
  current_timestamp AS _built_at
FROM {config.DB_PROCESSED}.fact_view_sessions s
JOIN {config.DB_PROCESSED}.dim_title t ON s.title_key = t.title_key
GROUP BY t.title_key, t.tconst, t.primary_title, t.title_type,
         cast(LEAST(CAST(s.completion_pct * 10 AS integer), 9) AS integer)
"""


VERIFY_QUERIES = [
    ("content_engagement_daily rows", f"SELECT count(*) AS n FROM {config.DB_REPORTING}.content_engagement_daily"),
    ("device_engagement_daily rows", f"SELECT count(*) AS n FROM {config.DB_REPORTING}.device_engagement_daily"),
    ("genre_mix_daily rows", f"SELECT count(*) AS n FROM {config.DB_REPORTING}.genre_mix_daily"),
    ("title_completion_funnel rows", f"SELECT count(*) AS n FROM {config.DB_REPORTING}.title_completion_funnel"),
    (
        "TOP 10 titles by total_watch_seconds",
        f"""SELECT primary_title, title_type, distinct_viewers, sessions_count,
                   total_watch_seconds, completion_rate
            FROM {config.DB_REPORTING}.content_engagement_daily
            ORDER BY total_watch_seconds DESC
            LIMIT 10""",
    ),
    (
        "Genre mix (top 10 by watch_seconds)",
        f"""SELECT genre, watch_seconds, pct_of_day
            FROM {config.DB_REPORTING}.genre_mix_daily
            ORDER BY watch_seconds DESC
            LIMIT 10""",
    ),
    (
        "Device engagement breakdown",
        f"""SELECT device_type, platform, sessions_count, distinct_viewers,
                   total_watch_seconds, completion_rate
            FROM {config.DB_REPORTING}.device_engagement_daily
            ORDER BY total_watch_seconds DESC""",
    ),
    (
        "Reconciliation: sessions in reporting vs processed",
        f"""SELECT
              (SELECT sum(sessions_count) FROM {config.DB_REPORTING}.content_engagement_daily) AS reporting_sessions,
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS processed_sessions""",
    ),
]


def main():
    print("=== Skill #06 — Reporting Zone (Iceberg pre-aggregates) ===")
    print()

    print("Step 1: Drop existing tables")
    for sql in DROP_STATEMENTS:
        table = sql.split()[-1]
        run_and_print(sql, f"  drop {table}", print_results=False)

    print()
    print("Step 2: Build pre-aggregates")
    run_and_print(CREATE_CONTENT_ENGAGEMENT_DAILY, "  CTAS content_engagement_daily", print_results=False)
    run_and_print(CREATE_DEVICE_ENGAGEMENT_DAILY, "  CTAS device_engagement_daily", print_results=False)
    run_and_print(CREATE_GENRE_MIX_DAILY, "  CTAS genre_mix_daily", print_results=False)
    run_and_print(CREATE_TITLE_COMPLETION_FUNNEL, "  CTAS title_completion_funnel", print_results=False)

    print()
    print("Step 3: Verify + showcase queries")
    for label, sql in VERIFY_QUERIES:
        run_and_print(sql, f"  {label}")

    print()
    print("=== Skill #06 complete ===")


if __name__ == "__main__":
    main()
