-- Per-device-type/platform daily engagement. Replaces the `device` block.
{{ config(
    unique_key=['engagement_date', 'device_type', 'platform'],
    partitioned_by=['engagement_date']
) }}

with sessions as (
    select s.session_start_date, s.customer_key, s.watch_duration_seconds,
           s.was_completed, d.device_type, d.platform
    from {{ source('processed', 'fact_view_sessions') }} s
    left join {{ source('processed', 'dim_device') }} d on s.device_key = d.device_key
    {% if is_incremental() and var('engagement_date', none) %}
    where s.session_start_date = date '{{ var("engagement_date") }}'
    {% endif %}
)

select
    session_start_date                               as engagement_date,
    device_type,
    platform,
    count(*)                                         as sessions_count,
    count(distinct customer_key)                     as distinct_viewers,
    cast(sum(watch_duration_seconds) as bigint)      as total_watch_seconds,
    cast(avg(watch_duration_seconds) as integer)     as avg_session_seconds,
    cast(sum(case when was_completed then 1 else 0 end) * 1.0 / count(*)
         as decimal(4, 3))                           as completion_rate,
    current_timestamp                                as _built_at
from sessions
group by 1, 2, 3
