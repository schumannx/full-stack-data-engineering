-- Daily genre share of watch time. Replaces the `genre_mix` block.
-- PySpark explode(split(genres, ',')) -> Athena CROSS JOIN UNNEST.
{{ config(
    unique_key=['engagement_date', 'genre'],
    partitioned_by=['engagement_date']
) }}

with sessions as (
    select s.session_start_date, s.watch_duration_seconds, t.genres
    from {{ source('processed', 'fact_view_sessions') }} s
    left join {{ source('processed', 'dim_title') }} t on s.title_key = t.title_key
    {% if is_incremental() and var('engagement_date', none) %}
    where s.session_start_date = date '{{ var("engagement_date") }}'
    {% endif %}
),
exploded as (
    select
        session_start_date as engagement_date,
        trim(g)            as genre,
        watch_duration_seconds
    from sessions
    cross join unnest(split(genres, ',')) as x (g)
),
totals as (
    select engagement_date, sum(watch_duration_seconds) as day_total
    from exploded
    group by 1
)

select
    e.engagement_date,
    e.genre,
    cast(sum(e.watch_duration_seconds) as bigint)                      as watch_seconds,
    cast(sum(e.watch_duration_seconds) * 100.0 / max(t.day_total)
         as decimal(5, 2))                                            as pct_of_day,
    current_timestamp                                                 as _built_at
from exploded e
join totals t on e.engagement_date = t.engagement_date
group by 1, 2
