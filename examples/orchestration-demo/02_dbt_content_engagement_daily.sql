-- dbt model: models/reporting/content_engagement_daily.sql
-- This replaces the `content = ...` block in glue/jobs/reporting_aggregates.py.
-- Adapter: dbt-athena (your tables are Iceberg in the Glue catalog, queried by Athena).
--
-- The config() block is the magic: it gives you table creation, partitioning, and
-- incremental MERGE for free. Your PySpark job hand-wrote all of that
-- (table_exists check + writeTo(...).overwritePartitions()). Here it's 5 lines of config.

{{
  config(
    materialized   = 'incremental',
    table_type     = 'iceberg',
    incremental_strategy = 'merge',
    unique_key     = ['engagement_date', 'title_key'],
    partitioned_by = ['engagement_date']
  )
}}

with sessions as (
    select *
    from {{ ref('fact_view_sessions') }}
    {% if is_incremental() %}
      -- only rebuild the target day; mirrors --engagement_date in the Glue job
      where session_start_date = date '{{ var("engagement_date") }}'
    {% endif %}
)

select
    s.session_start_date                                   as engagement_date,
    s.title_key, t.tconst, t.primary_title, t.title_type, t.genres,
    count(distinct s.customer_key)                         as distinct_viewers,
    count(*)                                               as sessions_count,
    cast(sum(s.watch_duration_seconds) as bigint)         as total_watch_seconds,
    sum(case when s.was_completed then 1 else 0 end)      as completion_count,
    cast(sum(case when s.was_completed then 1 else 0 end) * 1.0 / count(*)
         as decimal(4,3))                                 as completion_rate,
    cast(avg(s.watch_duration_seconds) as integer)        as avg_session_seconds
from sessions s
left join {{ ref('dim_title') }} t on s.title_key = t.title_key
group by 1, 2, 3, 4, 5, 6

-- Notice what's gone vs the PySpark version:
--   * no SparkSession setup / build_spark()
--   * no table_exists() branch, no writeTo()/overwritePartitions() — config() handles it
--   * `ref('fact_view_sessions')` builds the DAG edge automatically (Cosmos reads it)
