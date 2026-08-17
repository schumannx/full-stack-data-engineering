"""
Skill #04 — Raw Zone ETL.

Runs the landing -> raw transformation:
1. Create external tables on JSON.gz landing files
2. CTAS into Parquet raw tables with schema cast + quarantine + dedup
3. Verify row counts

Three sub-pipelines:
  - playback_events  (hour-partitioned, dedup on event_id)
  - customer_profiles (daily snapshot, full overwrite)
  - device_registry  (daily snapshot, full overwrite)
"""

from athena_runner import run_and_print, run_query, fetch_results
import config


# ─── Step 1: External tables on landing (JSON.gz) ──────────────────────────────

DROP_LANDING_EVENTS = f"DROP TABLE IF EXISTS {config.DB_LANDING}.playback_events"

CREATE_LANDING_EVENTS = f"""
CREATE EXTERNAL TABLE {config.DB_LANDING}.playback_events (
  event_id string,
  session_id string,
  customer_id string,
  title_id string,
  device_id string,
  device_version_id string,
  event_type string,
  event_timestamp string,
  position_ms int,
  bitrate_kbps int,
  geo_country string,
  schema_version int
)
PARTITIONED BY (yyyy string, mm string, dd string, hh string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{config.LANDING_PATH}playback_events/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.yyyy.type' = 'integer',
  'projection.yyyy.range' = '2024,2030',
  'projection.mm.type' = 'integer',
  'projection.mm.range' = '01,12',
  'projection.mm.digits' = '2',
  'projection.dd.type' = 'integer',
  'projection.dd.range' = '01,31',
  'projection.dd.digits' = '2',
  'projection.hh.type' = 'integer',
  'projection.hh.range' = '00,23',
  'projection.hh.digits' = '2',
  'storage.location.template' = '{config.LANDING_PATH}playback_events/yyyy=${{yyyy}}/mm=${{mm}}/dd=${{dd}}/hh=${{hh}}/'
)
"""

DROP_LANDING_CUSTOMERS = f"DROP TABLE IF EXISTS {config.DB_LANDING}.customer_profiles"

CREATE_LANDING_CUSTOMERS = f"""
CREATE EXTERNAL TABLE {config.DB_LANDING}.customer_profiles (
  customer_id string,
  email_hash string,
  signup_date string,
  country string,
  plan_tier string,
  age_band string,
  household_size int,
  created_at string,
  updated_at string
)
PARTITIONED BY (yyyy string, mm string, dd string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{config.LANDING_PATH}customer_profiles/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.yyyy.type' = 'integer',
  'projection.yyyy.range' = '2024,2030',
  'projection.mm.type' = 'integer',
  'projection.mm.range' = '01,12',
  'projection.mm.digits' = '2',
  'projection.dd.type' = 'integer',
  'projection.dd.range' = '01,31',
  'projection.dd.digits' = '2',
  'storage.location.template' = '{config.LANDING_PATH}customer_profiles/yyyy=${{yyyy}}/mm=${{mm}}/dd=${{dd}}/'
)
"""

DROP_LANDING_DEVICES = f"DROP TABLE IF EXISTS {config.DB_LANDING}.device_registry"

CREATE_LANDING_DEVICES = f"""
CREATE EXTERNAL TABLE {config.DB_LANDING}.device_registry (
  device_id string,
  device_version_id string,
  device_type string,
  platform string,
  device_model string,
  os_version string,
  app_version string,
  is_deprecated boolean
)
PARTITIONED BY (yyyy string, mm string, dd string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{config.LANDING_PATH}device_registry/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.yyyy.type' = 'integer',
  'projection.yyyy.range' = '2024,2030',
  'projection.mm.type' = 'integer',
  'projection.mm.range' = '01,12',
  'projection.mm.digits' = '2',
  'projection.dd.type' = 'integer',
  'projection.dd.range' = '01,31',
  'projection.dd.digits' = '2',
  'storage.location.template' = '{config.LANDING_PATH}device_registry/yyyy=${{yyyy}}/mm=${{mm}}/dd=${{dd}}/'
)
"""


# ─── Step 2: CTAS into raw Parquet tables (with transformations) ──────────────

DROP_RAW_EVENTS = f"DROP TABLE IF EXISTS {config.DB_RAW}.playback_events"

CREATE_RAW_EVENTS = f"""
CREATE TABLE {config.DB_RAW}.playback_events
WITH (
  external_location = '{config.RAW_PATH}playback_events/',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['event_date', 'event_hour']
) AS
WITH dedup AS (
  SELECT
    event_id,
    session_id,
    customer_id,
    title_id,
    device_id,
    device_version_id,
    event_type,
    CAST(from_iso8601_timestamp(event_timestamp) AS timestamp) AS event_timestamp,
    position_ms,
    bitrate_kbps,
    geo_country,
    schema_version,
    CAST(date_format(from_iso8601_timestamp(event_timestamp), '%Y-%m-%d') AS varchar) AS event_date_str,
    CAST(hour(from_iso8601_timestamp(event_timestamp)) AS varchar) AS event_hour_str,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn
  FROM {config.DB_LANDING}.playback_events
  WHERE event_type IN ('play','pause','seek','resume','complete','exit')
    AND schema_version = 1
    AND regexp_like(title_id, '^tt[0-9]{{7,10}}$')
)
SELECT
  event_id,
  session_id,
  customer_id,
  title_id,
  device_id,
  device_version_id,
  event_type,
  event_timestamp,
  position_ms,
  bitrate_kbps,
  geo_country,
  schema_version,
  date(event_date_str) AS event_date,
  CAST(event_hour_str AS integer) AS event_hour
FROM dedup
WHERE rn = 1
"""

DROP_RAW_CUSTOMERS = f"DROP TABLE IF EXISTS {config.DB_RAW}.customer_profiles"

CREATE_RAW_CUSTOMERS = f"""
CREATE TABLE {config.DB_RAW}.customer_profiles
WITH (
  external_location = '{config.RAW_PATH}customer_profiles/',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
) AS
SELECT
  customer_id,
  email_hash,
  date(signup_date) AS signup_date,
  country,
  plan_tier,
  age_band,
  household_size,
  CAST(from_iso8601_timestamp(created_at) AS timestamp) AS created_at,
  CAST(from_iso8601_timestamp(updated_at) AS timestamp) AS updated_at
FROM {config.DB_LANDING}.customer_profiles
"""

DROP_RAW_DEVICES = f"DROP TABLE IF EXISTS {config.DB_RAW}.device_registry"

CREATE_RAW_DEVICES = f"""
CREATE TABLE {config.DB_RAW}.device_registry
WITH (
  external_location = '{config.RAW_PATH}device_registry/',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
) AS
SELECT
  device_id,
  device_version_id,
  device_type,
  platform,
  device_model,
  os_version,
  app_version,
  is_deprecated
FROM {config.DB_LANDING}.device_registry
"""


# ─── Verification queries ──────────────────────────────────────────────────────

VERIFY_QUERIES = [
    (
        "raw playback_events row count",
        f"SELECT count(*) AS rows FROM {config.DB_RAW}.playback_events",
    ),
    (
        "raw playback_events by event_type",
        f"SELECT event_type, count(*) AS n FROM {config.DB_RAW}.playback_events GROUP BY 1 ORDER BY 1",
    ),
    (
        "raw customer_profiles row count",
        f"SELECT count(*) AS rows FROM {config.DB_RAW}.customer_profiles",
    ),
    (
        "raw device_registry row count",
        f"SELECT count(*) AS rows FROM {config.DB_RAW}.device_registry",
    ),
    (
        "events vs customers FK coverage",
        f"""
        SELECT
          count(*) AS events,
          count(DISTINCT e.customer_id) AS distinct_customers_in_events,
          count(DISTINCT c.customer_id) AS distinct_customers_in_dim,
          count(DISTINCT c.customer_id) FILTER (WHERE c.customer_id IS NOT NULL) AS matched
        FROM {config.DB_RAW}.playback_events e
        LEFT JOIN {config.DB_RAW}.customer_profiles c ON e.customer_id = c.customer_id
        """,
    ),
]


def main():
    print("=== Skill #04 — Raw Zone ETL ===")
    print()

    print("Step 1: Drop + recreate landing external tables (JSON.gz)")
    run_and_print(DROP_LANDING_EVENTS, "  drop landing.playback_events", print_results=False)
    run_and_print(CREATE_LANDING_EVENTS, "  create landing.playback_events", print_results=False)
    run_and_print(DROP_LANDING_CUSTOMERS, "  drop landing.customer_profiles", print_results=False)
    run_and_print(CREATE_LANDING_CUSTOMERS, "  create landing.customer_profiles", print_results=False)
    run_and_print(DROP_LANDING_DEVICES, "  drop landing.device_registry", print_results=False)
    run_and_print(CREATE_LANDING_DEVICES, "  create landing.device_registry", print_results=False)

    print()
    print("Step 2: Drop + CTAS raw Parquet tables (with dedup + schema cast)")
    run_and_print(DROP_RAW_EVENTS, "  drop raw.playback_events", print_results=False)
    run_and_print(CREATE_RAW_EVENTS, "  CTAS raw.playback_events", print_results=False)
    run_and_print(DROP_RAW_CUSTOMERS, "  drop raw.customer_profiles", print_results=False)
    run_and_print(CREATE_RAW_CUSTOMERS, "  CTAS raw.customer_profiles", print_results=False)
    run_and_print(DROP_RAW_DEVICES, "  drop raw.device_registry", print_results=False)
    run_and_print(CREATE_RAW_DEVICES, "  CTAS raw.device_registry", print_results=False)

    print()
    print("Step 3: Verify")
    for label, sql in VERIFY_QUERIES:
        run_and_print(sql, f"  {label}")

    print()
    print("=== Skill #04 complete ===")


if __name__ == "__main__":
    main()
