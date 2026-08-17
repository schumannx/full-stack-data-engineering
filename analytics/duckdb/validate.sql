-- Layered DuckDB validator for the Streaming DW (source -> raw -> processed -> reporting).
-- Reads the SAME Iceberg tables Athena serves, straight from S3 — free, out-of-band.
-- A cheap auditor that complements the in-pipeline gates (dq_check, dbt tests).
--
-- Run locally:  duckdb < analytics/duckdb/validate.sql      (creds from ~/.aws)
-- Run on EC2 :  duckdb < /opt/streaming/analytics/duckdb/validate.sql  (instance role)
--
-- Each check prints a number; the comment says the healthy value. Non-healthy = investigate.
-- Glue ATTACH is avoided (Glue us-east-1 vs data us-west-2); we read by S3 root.

LOAD httpfs; LOAD aws; LOAD iceberg;
CREATE SECRET IF NOT EXISTS aws_s3 (TYPE s3, PROVIDER credential_chain, REGION 'us-west-2');
SET unsafe_enable_version_guessing = true;  -- resolve latest snapshot from the table root

-- Table roots (note: dims are on the LEGACY processed/streaming path, not .db).
-- landing  s3://acme-dw-streaming-xs2026/streaming_landing.db/playback_events
-- raw      s3://acme-dw-streaming-xs2026/streaming_raw.db/playback_events
-- fact_ev  s3://acme-dw-streaming-xs2026/streaming_processed.db/fact_playback_events
-- fact_ses s3://acme-dw-streaming-xs2026/streaming_processed.db/fact_view_sessions
-- content  s3://acme-dw-streaming-xs2026/streaming_reporting.db/content_engagement_daily
-- genre    s3://acme-dw-streaming-xs2026/streaming_reporting.db/genre_mix_daily

.print ''
.print '################ SOURCE (landing) ################'
.print '--- freshness + volume + future-dated (future_dated should be 0) ---'
SELECT count(*) AS landing_rows,
       max(server_received_at) AS latest_arrival,
       count(*) FILTER (WHERE event_timestamp > server_received_at + INTERVAL 5 MINUTE) AS future_dated
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_landing.db/playback_events');

.print ''
.print '################ RAW ################'
.print '--- dedup worked? (dup_event_ids should be 0) ---'
SELECT count(*) AS raw_rows,
       count(*) - count(DISTINCT event_id) AS dup_event_ids
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_raw.db/playback_events');

.print '--- bad rows that should have been quarantined (all should be 0) ---'
SELECT
  count(*) FILTER (WHERE event_timestamp > server_received_at + INTERVAL 5 MINUTE) AS leaked_future,
  count(*) FILTER (WHERE NOT regexp_matches(title_id, '^tt[0-9]{7,10}$'))           AS leaked_bad_title,
  count(*) FILTER (WHERE event_id IS NULL)                                          AS leaked_null_id
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_raw.db/playback_events');

.print ''
.print '################ PROCESSED ################'
.print '--- referential integrity: orphan FKs (title/device should be 0; customer = no source-of-truth) ---'
SELECT
  count(*) FILTER (WHERE title_key  IS NULL) AS orphan_title,
  count(*) FILTER (WHERE device_key IS NULL) AS orphan_device,
  count(*) FILTER (WHERE customer_key IS NULL) AS orphan_customer
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_processed.db/fact_playback_events');

.print '--- watch-minutes / session sanity (all should be 0) ---'
SELECT
  count(*) FILTER (WHERE watch_duration_seconds < 0)                                  AS negative_watch,
  count(*) FILTER (WHERE completion_pct < 0 OR completion_pct > 1)                     AS bad_completion_pct,
  count(*) FILTER (WHERE session_end_ts < session_start_ts)                           AS end_before_start,
  -- NOTE: watch_duration_seconds is WALL-CLOCK (session_end-start, incl. pauses), so it
  -- legitimately exceeds content runtime — do NOT compare to runtime. completion_pct
  -- (position-based, checked above) is the real "watched past the end" guard. Here we
  -- only flag impossibly long sessions (>24h) = truly broken.
  count(*) FILTER (WHERE watch_duration_seconds > 86400)                              AS watch_over_24h
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_processed.db/fact_view_sessions');

.print ''
.print '################ REPORTING ################'
.print '--- CHECK: content mart sessions_count == recompute from fact (mismatches=0) ---'
WITH f AS (
  SELECT session_start_date AS engagement_date, title_key, count(*) AS sessions_count
  FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_processed.db/fact_view_sessions')
  GROUP BY 1, 2
),
m AS (
  SELECT engagement_date, title_key, sessions_count
  FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_reporting.db/content_engagement_daily')
)
SELECT (SELECT count(*) FROM m) AS mart_rows,
       (SELECT count(*) FROM f) AS fact_grain_rows,
       (SELECT count(*) FROM m JOIN f USING (engagement_date, title_key)
        WHERE m.sessions_count <> f.sessions_count) AS mismatches;

.print '--- bounds: completion_rate in [0,1] (bad_rows=0); genre pct sums ~100/day ---'
SELECT count(*) AS bad_completion_rate
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_reporting.db/content_engagement_daily')
WHERE completion_rate < 0 OR completion_rate > 1;
SELECT engagement_date, round(sum(pct_of_day), 1) AS pct_sum
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_reporting.db/genre_mix_daily')
GROUP BY 1 ORDER BY 1;

.print '--- DoD: day-over-day total sessions (eyeball for big swings) ---'
SELECT engagement_date,
       sum(sessions_count) AS sessions,
       sum(total_watch_seconds) AS watch_seconds
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_reporting.db/content_engagement_daily')
GROUP BY 1 ORDER BY 1;

.print ''
.print '--- SUMMARY: top 5 titles by watch time ---'
SELECT primary_title, sessions_count, total_watch_seconds, completion_rate
FROM iceberg_scan('s3://acme-dw-streaming-xs2026/streaming_reporting.db/content_engagement_daily')
ORDER BY total_watch_seconds DESC LIMIT 5;
