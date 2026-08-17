-- Completion deciles per title. Replaces the `title_completion_funnel` block.
{{ config(
    unique_key=['engagement_date', 'title_key', 'bucket'],
    partitioned_by=['engagement_date']
) }}

with sessions as (
    select s.session_start_date, s.title_key, s.completion_pct,
           t.tconst, t.primary_title, t.title_type
    from {{ source('processed', 'fact_view_sessions') }} s
    left join {{ source('processed', 'dim_title') }} t on s.title_key = t.title_key
    {% if is_incremental() and var('engagement_date', none) %}
    where s.session_start_date = date '{{ var("engagement_date") }}'
    {% endif %}
)

select
    session_start_date                              as engagement_date,
    title_key,
    tconst,
    primary_title,
    title_type,
    least(cast(completion_pct * 10 as integer), 9)  as bucket,
    count(*)                                        as sessions_in_bucket,
    current_timestamp                               as _built_at
from sessions
group by 1, 2, 3, 4, 5, 6
