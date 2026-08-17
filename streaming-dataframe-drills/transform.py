"""
transform.py — streaming-themed pandas transformations, sourced from data.py
==========================================================================

The seeded master dataset (customers, titles, devices, events) lives in
`data.py`. Each drill below pulls the slice it needs, applies one canonical
pandas pattern, and returns ``{"source": <df>, "result": <df>}`` for the
DuckDB-SQL validators in `validation.py`.

Run this file directly to print every drill's source and output:
    python transform.py
"""

import numpy as np
import pandas as pd

import data


# Master dataset, generated once at module import.
MASTER = data.generate()


# -----------------------------------------------------------------------------
# 01 — ROW_NUMBER: latest event per customer
# -----------------------------------------------------------------------------
def drill_01_row_number():
    """
    Pattern : Keep one row per group, picked by an ordering.
    Pandas  : sort_values + groupby().tail(1)
    SQL     : ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC) = 1
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002", "c003"])]
              [["customer_id", "event_time", "event_type"]]
              .sort_values(["customer_id", "event_time"])
              .reset_index(drop=True)
    )
    result = (
        source.sort_values(["customer_id", "event_time"])
              .groupby("customer_id", as_index=False).tail(1)
              .sort_values("customer_id")
              .reset_index(drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 02 — RANK vs DENSE_RANK (watch_seconds buckets guarantee natural ties)
# -----------------------------------------------------------------------------
def drill_02_rank_dense_rank():
    """
    Pattern : Rank with ties — RANK leaves gaps (1, 1, 3), DENSE_RANK doesn't (1, 1, 2).
    Pandas  : rank(method='min') and rank(method='dense')
    SQL     : RANK() vs DENSE_RANK()
    """
    events = MASTER["events"]
    source = events.head(8)[["event_id", "watch_seconds"]].reset_index(drop=True)
    result = source.copy()
    result["rank"]       = result["watch_seconds"].rank(method="min",   ascending=False).astype(int)
    result["dense_rank"] = result["watch_seconds"].rank(method="dense", ascending=False).astype(int)
    result = result.sort_values("event_id").reset_index(drop=True)
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 03 — LEAD: peek the next event per customer
# -----------------------------------------------------------------------------
def drill_03_lead():
    """
    Pattern : "What happens next in this group?"
    Pandas  : groupby().shift(-1)
    SQL     : LEAD(col) OVER (PARTITION BY customer_id ORDER BY event_time)
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002"])]
              [["customer_id", "event_time", "event_type"]]
              .sort_values(["customer_id", "event_time"])
              .reset_index(drop=True)
    )
    result = source.copy()
    result["next_event_type"] = source.groupby("customer_id")["event_type"].shift(-1)
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 04 — LAG: seconds since previous event per customer
# -----------------------------------------------------------------------------
def drill_04_lag():
    """
    Pattern : "How long since the previous row in this group?"
    Pandas  : groupby().shift(1), subtract.
    SQL     : EXTRACT(EPOCH FROM (col - LAG(col) OVER (...)))
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002"])]
              [["customer_id", "event_time"]]
              .sort_values(["customer_id", "event_time"])
              .reset_index(drop=True)
    )
    result = source.copy()
    prev = source.groupby("customer_id")["event_time"].shift(1)
    result["seconds_since_prev"] = (source["event_time"] - prev).dt.total_seconds()
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 05 — Running total: cumulative watch per customer
# -----------------------------------------------------------------------------
def drill_05_running_total():
    """
    Pattern : Cumulative sum within a group, ordered by time.
    Pandas  : groupby().cumsum()
    SQL     : SUM(col) OVER (PARTITION BY ... ORDER BY ... ROWS UNBOUNDED PRECEDING)
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002"])]
              [["customer_id", "event_time", "watch_seconds"]]
              .sort_values(["customer_id", "event_time"])
              .reset_index(drop=True)
    )
    result = source.copy()
    result["cumulative_watch"] = source.groupby("customer_id")["watch_seconds"].cumsum()
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 06 — Moving average: 3-day rolling daily watch per customer
# -----------------------------------------------------------------------------
def drill_06_moving_average():
    """
    Pattern : Rolling average over the last N rows in each group.
    Pandas  : groupby().rolling(window=N, min_periods=1).mean()
    SQL     : AVG(col) OVER (... ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)
    """
    events = MASTER["events"]
    daily = (
        events.assign(day=events["event_time"].dt.normalize())
              .groupby(["customer_id", "day"], as_index=False)["watch_seconds"].sum()
    )
    daily["watch_min"] = (daily["watch_seconds"] / 60).round(2)
    source = (
        daily[daily["customer_id"].isin(["c001", "c002"])]
             [["customer_id", "day", "watch_min"]]
             .sort_values(["customer_id", "day"]).reset_index(drop=True)
    )
    result = source.copy()
    result["rolling_avg_3"] = (
        source.groupby("customer_id")["watch_min"]
              .rolling(window=3, min_periods=1).mean()
              .reset_index(level=0, drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 07 — Cumulative max watch per customer
# -----------------------------------------------------------------------------
def drill_07_cumulative_max():
    """
    Pattern : Running max within a group.
    Pandas  : groupby().cummax()
    SQL     : MAX(col) OVER (... ROWS UNBOUNDED PRECEDING)
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002"])]
              [["customer_id", "event_time", "watch_seconds"]]
              .sort_values(["customer_id", "event_time"])
              .reset_index(drop=True)
    )
    result = source.copy()
    result["max_watch_so_far"] = source.groupby("customer_id")["watch_seconds"].cummax()
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 08 — Percent of group total
# -----------------------------------------------------------------------------
def drill_08_percent_of_group_total():
    """
    Pattern : Broadcast per-group sum back to each row, divide.
    Pandas  : groupby().transform('sum')
    SQL     : SUM(col) OVER (PARTITION BY group)
    """
    events = MASTER["events"]
    agg = (
        events.groupby(["customer_id", "title_id"], as_index=False)["watch_seconds"].sum()
    )
    source = (
        agg[agg["customer_id"].isin(["c001", "c002", "c003"])]
           .sort_values(["customer_id", "title_id"]).reset_index(drop=True)
    )
    result = source.copy()
    result["customer_total"]        = source.groupby("customer_id")["watch_seconds"].transform("sum")
    result["pct_of_customer_total"] = source["watch_seconds"] / result["customer_total"]
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 09 — FIRST_VALUE / LAST_VALUE per (customer, title)
# -----------------------------------------------------------------------------
def drill_09_first_last_value():
    """
    Pattern : First and last value in each ordered group.
    Pandas  : groupby().agg(first=..., last=...) on sorted data
    SQL     : FIRST_VALUE / LAST_VALUE — LAST_VALUE needs the full
              UNBOUNDED-PRECEDING-TO-UNBOUNDED-FOLLOWING frame; its default
              stops at CURRENT ROW (a top SQL gotcha).
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"].isin(["c001", "c002"])]
              [["customer_id", "title_id", "event_time", "event_type"]]
              .sort_values(["customer_id", "title_id", "event_time"])
              .reset_index(drop=True)
    )
    result = (
        source.sort_values(["customer_id", "title_id", "event_time"])
              .groupby(["customer_id", "title_id"], as_index=False)
              .agg(first_event=("event_type", "first"),
                   last_event=("event_type", "last"))
              .sort_values(["customer_id", "title_id"]).reset_index(drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 10 — Top-N per group: top 2 titles per genre by total watch
# -----------------------------------------------------------------------------
def drill_10_top_n_per_group():
    """
    Pattern : Top N rows per group by a metric (with deterministic tie-breaker).
    Pandas  : sort_values + groupby().head(N)
    SQL     : ROW_NUMBER() OVER (PARTITION BY group ORDER BY metric DESC, key ASC) <= N
    """
    events = MASTER["events"]
    titles = MASTER["titles"]
    source = (
        events.groupby("title_id", as_index=False)["watch_seconds"].sum()
              .merge(titles[["title_id", "genre"]], on="title_id")
              [["title_id", "genre", "watch_seconds"]]
              .sort_values(["genre", "watch_seconds", "title_id"], ascending=[True, False, True])
              .reset_index(drop=True)
    )
    result = (
        source.groupby("genre", as_index=False).head(2)
              .reset_index(drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 11 — Groupby aggregation: per-genre stats
# -----------------------------------------------------------------------------
def drill_11_groupby_agg():
    """
    Pattern : Per-category roll-up.
    Pandas  : groupby().agg(name=(col, fn))
    SQL     : GROUP BY ... SUM / AVG / COUNT
    """
    events = MASTER["events"]
    titles = MASTER["titles"]
    source = (
        events.merge(titles[["title_id", "genre"]], on="title_id")
              [["title_id", "genre", "watch_seconds"]]
    )
    result = (
        source.groupby("genre", as_index=False)
              .agg(total_seconds=("watch_seconds", "sum"),
                   mean_seconds=("watch_seconds", "mean"),
                   n_views=("watch_seconds", "size"))
              .sort_values("genre").reset_index(drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 12 — Groupby transform: broadcast genre avg back to each event
# -----------------------------------------------------------------------------
def drill_12_groupby_transform():
    """
    Pattern : Compute a group stat and broadcast it back to every row.
    Pandas  : groupby()[col].transform('mean')
    SQL     : AVG(col) OVER (PARTITION BY group)
    """
    events = MASTER["events"]
    titles = MASTER["titles"]
    source = (
        events.merge(titles[["title_id", "genre"]], on="title_id")
              [["event_id", "title_id", "genre", "watch_seconds"]]
              .sort_values("event_id").reset_index(drop=True)
    )
    result = source.copy()
    result["genre_avg"] = source.groupby("genre")["watch_seconds"].transform("mean")
    result["above_avg"] = source["watch_seconds"] > result["genre_avg"]
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 13 — HAVING: genres with > 2 titles
# -----------------------------------------------------------------------------
def drill_13_having():
    """
    Pattern : Filter groups by an aggregate condition.
    Pandas  : groupby + agg + filter
    SQL     : GROUP BY ... HAVING COUNT(*) > N
    """
    titles = MASTER["titles"]
    source = titles[["title_id", "genre"]]
    counts = source.groupby("genre", as_index=False).size().rename(columns={"size": "n_titles"})
    result = counts[counts["n_titles"] > 2].sort_values("genre").reset_index(drop=True)
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 14 — Pivot / unpivot: daily watch_min by device_type
# -----------------------------------------------------------------------------
def drill_14_pivot_unpivot():
    """
    Pattern : Reshape long ↔ wide.
    Pandas  : pivot_table / melt
    SQL     : PIVOT / UNPIVOT (DuckDB has both as first-class syntax)
    """
    events  = MASTER["events"]
    devices = MASTER["devices"]
    enriched = events.merge(devices[["device_id", "device_type"]], on="device_id")
    enriched["day"] = enriched["event_time"].dt.normalize()
    daily = (
        enriched.groupby(["day", "device_type"], as_index=False)["watch_seconds"].sum()
    )
    daily["watch_min"] = (daily["watch_seconds"] / 60).round(2)
    source = daily[["day", "device_type", "watch_min"]].sort_values(["day", "device_type"]).reset_index(drop=True)

    wide = (
        source.pivot_table(index="day", columns="device_type", values="watch_min", aggfunc="sum")
              .reset_index()
    )
    wide.columns.name = None
    wide = wide.sort_values("day").reset_index(drop=True)

    long_back = (
        wide.melt(id_vars="day", var_name="device_type", value_name="watch_min")
            .dropna(subset=["watch_min"])
            .sort_values(["day", "device_type"]).reset_index(drop=True)
    )
    return {"source": source, "wide": wide, "long": long_back}


# -----------------------------------------------------------------------------
# 15 — Sessionize: gap-and-island on one customer's events
# -----------------------------------------------------------------------------
def drill_15_sessionize():
    """
    Pattern : Group events into sessions by inactivity gap.
    Pandas  : groupby().shift(1), gap mask, cumsum()
    SQL     : LAG to compute gap → CASE flag → SUM() OVER as running counter.
    """
    GAP_MIN = 30
    events = MASTER["events"]
    source = (
        events[events["customer_id"] == "c001"]
              [["customer_id", "event_time"]]
              .sort_values(["customer_id", "event_time"]).reset_index(drop=True)
    )
    prev = source.groupby("customer_id")["event_time"].shift(1)
    gap_min = (source["event_time"] - prev).dt.total_seconds() / 60
    new_session = gap_min.isna() | (gap_min > GAP_MIN)
    result = source.copy()
    result["session_num"] = new_session.groupby(source["customer_id"]).cumsum().astype(int)
    return {"source": source, "result": result, "gap_minutes": GAP_MIN}


# -----------------------------------------------------------------------------
# 16 — Deduplicate, keep latest per (customer_id, title_id)
# -----------------------------------------------------------------------------
def drill_16_dedup_latest():
    """
    Pattern : Multiple events for the same key — keep the latest.
    Pandas  : sort_values + drop_duplicates(subset=key, keep='last')
    SQL     : ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) = 1

    Within a session, each (customer_id, title_id) already has multiple
    events (start, pause, stop, ...). Across sessions a customer may
    re-watch the same title. Both produce real duplicate keys to dedup.
    """
    events = MASTER["events"]
    source = (
        events[events["customer_id"] == "c001"]
              [["customer_id", "title_id", "event_time", "position_sec"]]
              .sort_values(["customer_id", "title_id", "event_time"]).reset_index(drop=True)
    )
    result = (
        source.sort_values(["customer_id", "title_id", "event_time"])
              .drop_duplicates(subset=["customer_id", "title_id"], keep="last")
              .sort_values(["customer_id", "title_id"]).reset_index(drop=True)
    )
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 17 — Null detection (the validation drill — injects nulls deliberately)
# -----------------------------------------------------------------------------
def drill_17_null_detection():
    """
    Pattern : Data quality — flag rows with any null, then count per column.
    Pandas  : isna().any(axis=1) / isna().sum()
    SQL     : col IS NULL / COUNT(*) FILTER (WHERE col IS NULL)

    Master data is clean. The drill injects 4 nulls into a small slice so
    the demo is fully self-explanatory.
    """
    events = MASTER["events"]
    source = events.head(5)[["event_id", "customer_id", "title_id", "position_sec"]].reset_index(drop=True).copy()
    source["position_sec"] = source["position_sec"].astype(float)  # so NaN can live here
    source.loc[1, "customer_id"]  = None
    source.loc[2, "title_id"]     = None
    source.loc[3, "position_sec"] = np.nan
    source.loc[4, "customer_id"]  = None

    critical = ["customer_id", "title_id", "position_sec"]
    per_row = source.copy()
    per_row["has_null"] = source[critical].isna().any(axis=1)
    per_col = (
        source[critical].isna().sum()
        .rename("null_count").reset_index().rename(columns={"index": "col_name"})
        .sort_values("col_name").reset_index(drop=True)
    )
    return {"source": source, "per_row": per_row, "per_col": per_col, "critical": critical}


# -----------------------------------------------------------------------------
# 18 — Funnel: completion-step counts per (customer, title) session
# -----------------------------------------------------------------------------
def drill_18_funnel_steps():
    """
    Pattern : Count sessions reaching each milestone.
    Pandas  : Boolean masks summed.
    SQL     : COUNT(*) FILTER (WHERE max_pct >= threshold)
    """
    events = MASTER["events"]
    titles = MASTER["titles"]
    enriched = events.merge(titles[["title_id", "runtime_min"]], on="title_id")
    enriched["pct"] = (enriched["position_sec"] / (enriched["runtime_min"] * 60)).clip(upper=1.0)
    source = (
        enriched.groupby(["customer_id", "title_id"], as_index=False)["pct"].max()
                .rename(columns={"pct": "max_pct"})
    )
    result = pd.DataFrame({
        "step":       ["started", "p25", "p50", "p100"],
        "n_sessions": [
            int((source["max_pct"] > 0).sum()),
            int((source["max_pct"] >= 0.25).sum()),
            int((source["max_pct"] >= 0.50).sum()),
            int((source["max_pct"] >= 1.00).sum()),
        ],
    })
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 19 — Percentile per group: P50 / P90 watch_seconds per genre
# -----------------------------------------------------------------------------
def drill_19_percentile_per_group():
    """
    Pattern : P50 / P90 per category.
    Pandas  : groupby().quantile([0.5, 0.9]).unstack()
    SQL     : QUANTILE_CONT(col, q)  (DuckDB; ANSI: PERCENTILE_CONT WITHIN GROUP)
    """
    events = MASTER["events"]
    titles = MASTER["titles"]
    source = (
        events.merge(titles[["title_id", "genre"]], on="title_id")
              [["genre", "watch_seconds"]]
    )
    q = source.groupby("genre")["watch_seconds"].quantile([0.5, 0.9]).unstack()
    q.columns = ["p50", "p90"]
    result = q.reset_index().sort_values("genre").reset_index(drop=True)
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# 20 — Day-over-day delta: DAU series
# -----------------------------------------------------------------------------
def drill_20_day_over_day():
    """
    Pattern : Compare each day to the previous day.
    Pandas  : shift(1), subtract, divide.
    SQL     : LAG() OVER (ORDER BY day)
    """
    events = MASTER["events"]
    enriched = events.copy()
    enriched["day"] = enriched["event_time"].dt.normalize()
    source = (
        enriched.groupby("day", as_index=False)["customer_id"].nunique()
                .rename(columns={"customer_id": "dau"})
    )
    result = source.sort_values("day").reset_index(drop=True)
    result["prev_dau"]   = result["dau"].shift(1)
    result["delta"]      = result["dau"] - result["prev_dau"]
    result["pct_change"] = result["delta"] / result["prev_dau"]
    return {"source": source, "result": result}


# -----------------------------------------------------------------------------
# Drill registry — consumed by validation.py and the __main__ block below.
# -----------------------------------------------------------------------------
DRILLS = [
    ("01_row_number",           drill_01_row_number),
    ("02_rank_dense_rank",      drill_02_rank_dense_rank),
    ("03_lead",                 drill_03_lead),
    ("04_lag",                  drill_04_lag),
    ("05_running_total",        drill_05_running_total),
    ("06_moving_average",       drill_06_moving_average),
    ("07_cumulative_max",       drill_07_cumulative_max),
    ("08_percent_of_group",     drill_08_percent_of_group_total),
    ("09_first_last_value",     drill_09_first_last_value),
    ("10_top_n_per_group",      drill_10_top_n_per_group),
    ("11_groupby_agg",          drill_11_groupby_agg),
    ("12_groupby_transform",    drill_12_groupby_transform),
    ("13_having",               drill_13_having),
    ("14_pivot_unpivot",        drill_14_pivot_unpivot),
    ("15_sessionize",           drill_15_sessionize),
    ("16_dedup_latest",         drill_16_dedup_latest),
    ("17_null_detection",       drill_17_null_detection),
    ("18_funnel_steps",         drill_18_funnel_steps),
    ("19_percentile_per_group", drill_19_percentile_per_group),
    ("20_day_over_day",         drill_20_day_over_day),
]


if __name__ == "__main__":
    pd.set_option("display.width", 130)
    pd.set_option("display.max_columns", 20)
    print("=== Master data (data.py) ===")
    for name, df in MASTER.items():
        print(f"\n--- {name} ({len(df)} rows) ---")
        print(df.head(6))
    print("\n\n=== Drills ===")
    for name, fn in DRILLS:
        out = fn()
        print(f"\n--- {name} ---")
        print("source:")
        print(out["source"].head(10))
        for k, v in out.items():
            if k == "source" or not hasattr(v, "to_string"):
                continue
            print(f"\n{k}:")
            print(v.head(10))
