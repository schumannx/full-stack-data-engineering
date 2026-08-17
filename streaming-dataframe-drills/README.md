# Streaming DataFrame Drills

A learning sandbox for the canonical window-function and aggregation patterns
that show up everywhere in data engineering, paired side-by-side as
**pandas transformations** validated by **DuckDB SQL queries**.

## The principle

This sandbox follows one rule, set by a DE mentor:

- **Dataframes do the data work.** Each drill builds a small synthetic
  streaming-themed dataset (playback events, titles, customers, devices) and
  transforms it with pandas.
- **SQL does the validation.** A parallel DuckDB query re-derives the
  answer or asserts a property of it. The drill passes only if pandas and
  SQL agree.

This mirrors how mature DE pipelines look in practice: dataframes (or
PySpark, or Polars) carry the in-memory transformation; SQL queries do the
contract tests and assertions.

## Why pandas + DuckDB specifically

- **pandas** is the lingua franca for in-memory tabular data in Python —
  excellent for problems that fit in RAM (2D-ish tasks).
- **DuckDB** runs full SQL against pandas DataFrames in-process — no server,
  no extract step. Perfect for validation queries that live right next to the
  data.

SQL is the long-lived skill. Window functions you learn here transfer to
Snowflake, BigQuery, Redshift, Spark SQL, Postgres, and any warehouse you'll
touch for the next decade.

## Layout

```
streaming-dataframe-drills/
├── README.md           ← you are here
├── requirements.txt    ← pandas, duckdb, pyarrow
├── transform.py        ← all data generation + pandas transformations (20 drills)
└── validation.py       ← DuckDB SQL counter-implementations + assertions
```

Two files, one role each. `transform.py` is the dataframe world;
`validation.py` is the SQL world.

## Setup

```bash
pip install -r requirements.txt
```

Python 3.10+. All compute is in-memory — no S3, no warehouse, no servers.

## Study mode — see the inputs and outputs

```bash
python transform.py
```

Prints every drill's source dataframe and its transformed result. Use this
to read through the curriculum and build intuition.

## Validation mode — prove pandas matches SQL

```bash
python validation.py
```

For each drill, this runs the SQL counter-implementation and asserts the
two results are equal. Ends with `ALL 20 DRILLS VALIDATED`.

## What's in here

20 drills covering the canonical window / groupby / validation patterns:

| #  | Pattern                              | SQL equivalent                                |
|----|--------------------------------------|-----------------------------------------------|
| 01 | Latest row per group                 | `ROW_NUMBER()`                                |
| 02 | Rank vs dense rank                   | `RANK`, `DENSE_RANK`                          |
| 03 | Peek next row                        | `LEAD`                                        |
| 04 | Time-since-previous                  | `LAG`                                         |
| 05 | Running total                        | `SUM() OVER ... UNBOUNDED PRECEDING`          |
| 06 | Moving average                       | `AVG() OVER ... N PRECEDING`                  |
| 07 | Cumulative max                       | `MAX() OVER ...`                              |
| 08 | Percent of group total               | `SUM() OVER (PARTITION BY ...)`               |
| 09 | First / last value per partition     | `FIRST_VALUE`, `LAST_VALUE`                   |
| 10 | Top-N per group                      | `ROW_NUMBER() <= N`                           |
| 11 | Groupby aggregation                  | `GROUP BY`                                    |
| 12 | Broadcast group stat back to rows    | `AVG() OVER (PARTITION BY ...)`               |
| 13 | Filter on aggregates                 | `HAVING`                                      |
| 14 | Pivot / unpivot                      | `PIVOT`, `UNPIVOT`                            |
| 15 | Sessionization (gap-and-island)      | `LAG` + running `SUM`                         |
| 16 | Deduplicate, keep latest             | `ROW_NUMBER() = 1`                            |
| 17 | Null detection (validation drill)    | `IS NULL`, `COUNT(*) FILTER`                  |
| 18 | Funnel step counts                   | `COUNT(*) FILTER (WHERE ...)`                 |
| 19 | Percentile per group                 | `QUANTILE_CONT`                               |
| 20 | Day-over-day delta                   | `LAG`                                         |

## Reading order

If you're new to window functions, start at **01** and walk forward.

- **01 → 10**: core window machinery (`ROW_NUMBER`, `RANK`, `LEAD`, `LAG`,
  framed aggregates, partition-broadcast).
- **11 → 13**: grouping basics (`GROUP BY`, `HAVING`, broadcast via
  `transform` vs `OVER`).
- **14 → 20**: higher-leverage analytic patterns (pivots, sessionization,
  dedup, null checks, funnels, percentiles, deltas).

## How a drill is structured

**In `transform.py`** — one function per drill:

```python
def drill_03_lead():
    events = pd.DataFrame({...})                            # build sample data
    result = events.sort_values(...).reset_index(drop=True) # transform with pandas
    result["next_event_type"] = result.groupby("session_id")["event_type"].shift(-1)
    return {"source": events, "result": result}             # both for validator
```

**In `validation.py`** — one matching function per drill:

```python
def validate_03_lead():
    d = transform.drill_03_lead()
    events = d["source"]                                    # DuckDB sees this name
    sql = duckdb.sql("""
        SELECT session_id, event_time, event_type,
               LEAD(event_type) OVER (PARTITION BY session_id ORDER BY event_time) AS next_event_type
        FROM events ORDER BY session_id, event_time
    """).df()
    _assert_eq(d["result"], sql, "03_lead")
```

## How to add a new drill

1. Add `drill_NN_<name>()` to `transform.py` returning `{"source": ..., "result": ...}`.
2. Add it to the `DRILLS` registry at the bottom of `transform.py`.
3. Add `validate_NN_<name>()` to `validation.py` with the SQL counter-implementation.
4. Add it to the `VALIDATORS` list at the bottom of `validation.py`.
5. Prefer **clarity over cleverness** in the pandas code — this is for learning.
