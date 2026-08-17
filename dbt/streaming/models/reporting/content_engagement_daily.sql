-- Per-title daily engagement. Replaces the `content` block of reporting_aggregates.py.
{{ config(
    unique_key=['engagement_date', 'title_key'],
    partitioned_by=['engagement_date']
) }}

with sessions as (
    select *
    from {{ source('processed', 'fact_view_sessions') }}
    {% if is_incremental() and var('engagement_date', none) %}
    where session_start_date = date '{{ var("engagement_date") }}'
    {% endif %}
),
titles as (
    select title_key, tconst, primary_title, title_type, genres
    from {{ source('processed', 'dim_title') }}
)

select
    s.session_start_date                              as engagement_date,
    s.title_key,
    t.tconst,
    t.primary_title,
    t.title_type,
    t.genres,
    count(distinct s.customer_key)                    as distinct_viewers,
    count(*)                                          as sessions_count,
    cast(sum(s.watch_duration_seconds) as bigint)    as total_watch_seconds,
    sum(case when s.was_completed then 1 else 0 end)  as completion_count,
    cast(sum(case when s.was_completed then 1 else 0 end) * 1.0 / count(*)
         as decimal(4, 3))                            as completion_rate,
    cast(avg(s.watch_duration_seconds) as integer)    as avg_session_seconds,
    current_timestamp                                 as _built_at
from sessions s
left join titles t on s.title_key = t.title_key
group by 1, 2, 3, 4, 5, 6
