"""
validation.py — DuckDB SQL counter-implementations that prove transform.py is right
==================================================================================

For each drill in transform.py, this file re-derives the answer with SQL
(window functions, GROUP BY, etc.) and asserts that the pandas result matches.

This is the mentor's rule made concrete:
    dataframes for the data, SQL for the validation.

Run:
    python validation.py
"""

import pandas as pd
import duckdb

import transform


def _assert_eq(left, right, label):
    pd.testing.assert_frame_equal(
        left.reset_index(drop=True),
        right.reset_index(drop=True),
        check_dtype=False,
    )
    print(f"PASS — {label}")


# -----------------------------------------------------------------------------
def validate_01_row_number():
    d = transform.drill_01_row_number()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, event_time, event_type
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC) AS rn
            FROM source
        ) WHERE rn = 1 ORDER BY customer_id
    """).df()
    _assert_eq(d["result"], sql, "01_row_number")


def validate_02_rank_dense_rank():
    d = transform.drill_02_rank_dense_rank()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT event_id, watch_seconds,
               RANK()       OVER (ORDER BY watch_seconds DESC) AS rank,
               DENSE_RANK() OVER (ORDER BY watch_seconds DESC) AS dense_rank
        FROM source ORDER BY event_id
    """).df()
    _assert_eq(d["result"], sql, "02_rank_dense_rank")


def validate_03_lead():
    d = transform.drill_03_lead()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, event_time, event_type,
               LEAD(event_type) OVER (PARTITION BY customer_id ORDER BY event_time) AS next_event_type
        FROM source ORDER BY customer_id, event_time
    """).df()
    _assert_eq(d["result"], sql, "03_lead")


def validate_04_lag():
    d = transform.drill_04_lag()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, event_time,
               EXTRACT(EPOCH FROM (event_time
                   - LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time))) AS seconds_since_prev
        FROM source ORDER BY customer_id, event_time
    """).df()
    _assert_eq(d["result"], sql, "04_lag")


def validate_05_running_total():
    d = transform.drill_05_running_total()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, event_time, watch_seconds,
               SUM(watch_seconds) OVER (
                   PARTITION BY customer_id ORDER BY event_time
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cumulative_watch
        FROM source ORDER BY customer_id, event_time
    """).df()
    _assert_eq(d["result"], sql, "05_running_total")


def validate_06_moving_average():
    d = transform.drill_06_moving_average()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, day, watch_min,
               AVG(watch_min) OVER (
                   PARTITION BY customer_id ORDER BY day
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
               ) AS rolling_avg_3
        FROM source ORDER BY customer_id, day
    """).df()
    _assert_eq(d["result"], sql, "06_moving_average")


def validate_07_cumulative_max():
    d = transform.drill_07_cumulative_max()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, event_time, watch_seconds,
               MAX(watch_seconds) OVER (
                   PARTITION BY customer_id ORDER BY event_time
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS max_watch_so_far
        FROM source ORDER BY customer_id, event_time
    """).df()
    _assert_eq(d["result"], sql, "07_cumulative_max")


def validate_08_percent_of_group():
    d = transform.drill_08_percent_of_group_total()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, title_id, watch_seconds,
               SUM(watch_seconds) OVER (PARTITION BY customer_id) AS customer_total,
               watch_seconds * 1.0 / SUM(watch_seconds) OVER (PARTITION BY customer_id)
                   AS pct_of_customer_total
        FROM source ORDER BY customer_id, title_id
    """).df()
    _assert_eq(d["result"], sql, "08_percent_of_group")


def validate_09_first_last_value():
    d = transform.drill_09_first_last_value()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        WITH framed AS (
            SELECT customer_id, title_id,
                   FIRST_VALUE(event_type) OVER w AS first_event,
                   LAST_VALUE(event_type)  OVER w AS last_event,
                   ROW_NUMBER() OVER (PARTITION BY customer_id, title_id ORDER BY event_time) AS rn
            FROM source
            WINDOW w AS (
                PARTITION BY customer_id, title_id ORDER BY event_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        )
        SELECT customer_id, title_id, first_event, last_event
        FROM framed WHERE rn = 1 ORDER BY customer_id, title_id
    """).df()
    _assert_eq(d["result"], sql, "09_first_last_value")


def validate_10_top_n_per_group():
    d = transform.drill_10_top_n_per_group()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT title_id, genre, watch_seconds
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY genre ORDER BY watch_seconds DESC, title_id ASC
            ) AS rn
            FROM source
        ) WHERE rn <= 2
        ORDER BY genre, watch_seconds DESC, title_id ASC
    """).df()
    _assert_eq(d["result"], sql, "10_top_n_per_group")


def validate_11_groupby_agg():
    d = transform.drill_11_groupby_agg()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT genre,
               SUM(watch_seconds) AS total_seconds,
               AVG(watch_seconds) AS mean_seconds,
               COUNT(*)           AS n_views
        FROM source GROUP BY genre ORDER BY genre
    """).df()
    _assert_eq(d["result"], sql, "11_groupby_agg")


def validate_12_groupby_transform():
    d = transform.drill_12_groupby_transform()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT event_id, title_id, genre, watch_seconds,
               AVG(watch_seconds) OVER (PARTITION BY genre) AS genre_avg,
               watch_seconds > AVG(watch_seconds) OVER (PARTITION BY genre) AS above_avg
        FROM source ORDER BY event_id
    """).df()
    _assert_eq(d["result"], sql, "12_groupby_transform")


def validate_13_having():
    d = transform.drill_13_having()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT genre, COUNT(*) AS n_titles
        FROM source GROUP BY genre HAVING COUNT(*) > 2
        ORDER BY genre
    """).df()
    _assert_eq(d["result"], sql, "13_having")


def validate_14_pivot_unpivot():
    d = transform.drill_14_pivot_unpivot()
    source = d["source"]                                                    # noqa: F841
    sql_wide = duckdb.sql("""
        PIVOT source ON device_type USING SUM(watch_min) GROUP BY day ORDER BY day
    """).df()
    _assert_eq(d["wide"].sort_index(axis=1), sql_wide.sort_index(axis=1), "14_pivot_wide")

    pandas_wide = d["wide"]                                                 # noqa: F841
    device_cols = [c for c in pandas_wide.columns if c != "day"]
    # DuckDB UNPIVOT drops rows whose value column is NULL; the transform
    # drops them via .dropna() to keep the two sides in sync.
    sql_long = duckdb.sql(f"""
        UNPIVOT pandas_wide
        ON {', '.join(device_cols)}
        INTO NAME device_type VALUE watch_min
        ORDER BY day, device_type
    """).df()
    _assert_eq(d["long"], sql_long, "14_pivot_long")


def validate_15_sessionize():
    d = transform.drill_15_sessionize()
    source = d["source"]                                                    # noqa: F841
    gap_min = d["gap_minutes"]
    sql = duckdb.sql(f"""
        WITH gapped AS (
            SELECT customer_id, event_time,
                   EXTRACT(EPOCH FROM (event_time
                       - LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time))) / 60 AS gap_min
            FROM source
        ),
        flagged AS (
            SELECT *, CASE WHEN gap_min IS NULL OR gap_min > {gap_min} THEN 1 ELSE 0 END AS is_new_session
            FROM gapped
        )
        SELECT customer_id, event_time,
               SUM(is_new_session) OVER (
                   PARTITION BY customer_id ORDER BY event_time
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS session_num
        FROM flagged ORDER BY customer_id, event_time
    """).df()
    _assert_eq(d["result"][["customer_id","event_time","session_num"]], sql, "15_sessionize")


def validate_16_dedup_latest():
    d = transform.drill_16_dedup_latest()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT customer_id, title_id, event_time, position_sec
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, title_id ORDER BY event_time DESC) AS rn
            FROM source
        ) WHERE rn = 1
        ORDER BY customer_id, title_id
    """).df()
    _assert_eq(d["result"], sql, "16_dedup_latest")


def validate_17_null_detection():
    d = transform.drill_17_null_detection()
    source = d["source"]                                                    # noqa: F841
    sql_per_row = duckdb.sql("""
        SELECT event_id, customer_id, title_id, position_sec,
               (customer_id IS NULL OR title_id IS NULL OR position_sec IS NULL) AS has_null
        FROM source ORDER BY event_id
    """).df()
    _assert_eq(d["per_row"], sql_per_row, "17_null_detection_per_row")

    sql_per_col = duckdb.sql("""
        SELECT 'customer_id'  AS col_name, COUNT(*) FILTER (WHERE customer_id  IS NULL) AS null_count FROM source
        UNION ALL
        SELECT 'position_sec',              COUNT(*) FILTER (WHERE position_sec IS NULL)               FROM source
        UNION ALL
        SELECT 'title_id',                  COUNT(*) FILTER (WHERE title_id     IS NULL)               FROM source
        ORDER BY col_name
    """).df()
    _assert_eq(d["per_col"], sql_per_col, "17_null_detection_per_col")


def validate_18_funnel_steps():
    d = transform.drill_18_funnel_steps()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT 'started' AS step, COUNT(*) FILTER (WHERE max_pct > 0)    AS n_sessions FROM source
        UNION ALL SELECT 'p25',   COUNT(*) FILTER (WHERE max_pct >= 0.25)              FROM source
        UNION ALL SELECT 'p50',   COUNT(*) FILTER (WHERE max_pct >= 0.50)              FROM source
        UNION ALL SELECT 'p100',  COUNT(*) FILTER (WHERE max_pct >= 1.00)              FROM source
    """).df()
    _assert_eq(d["result"], sql, "18_funnel_steps")


def validate_19_percentile_per_group():
    d = transform.drill_19_percentile_per_group()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT genre,
               QUANTILE_CONT(watch_seconds, 0.5) AS p50,
               QUANTILE_CONT(watch_seconds, 0.9) AS p90
        FROM source GROUP BY genre ORDER BY genre
    """).df()
    _assert_eq(d["result"], sql, "19_percentile_per_group")


def validate_20_day_over_day():
    d = transform.drill_20_day_over_day()
    source = d["source"]                                                    # noqa: F841
    sql = duckdb.sql("""
        SELECT day, dau,
               LAG(dau) OVER (ORDER BY day) AS prev_dau,
               dau - LAG(dau) OVER (ORDER BY day) AS delta,
               (dau - LAG(dau) OVER (ORDER BY day)) * 1.0
                   / LAG(dau) OVER (ORDER BY day) AS pct_change
        FROM source ORDER BY day
    """).df()
    _assert_eq(d["result"], sql, "20_day_over_day")


VALIDATORS = [
    validate_01_row_number, validate_02_rank_dense_rank, validate_03_lead,
    validate_04_lag, validate_05_running_total, validate_06_moving_average,
    validate_07_cumulative_max, validate_08_percent_of_group,
    validate_09_first_last_value, validate_10_top_n_per_group,
    validate_11_groupby_agg, validate_12_groupby_transform,
    validate_13_having, validate_14_pivot_unpivot, validate_15_sessionize,
    validate_16_dedup_latest, validate_17_null_detection,
    validate_18_funnel_steps, validate_19_percentile_per_group,
    validate_20_day_over_day,
]


if __name__ == "__main__":
    for fn in VALIDATORS:
        fn()
    print("\nALL 20 DRILLS VALIDATED")
