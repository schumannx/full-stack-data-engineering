"""Cross-zone reconciliation assertions — compute-agnostic.

Ported from streaming-etl/validate_etl.py so the same checks run whether
the transform layer is Athena (legacy) or Glue PySpark (v2). The daily_rollup DAG
calls run_all() as a gating task before the reporting refresh (DESIGN.md §6.2).

I/O is injected: ``run_query(sql) -> list[dict-like rows]``. That keeps this
module dependency-free and unit-testable with a fake query function. In Airflow,
pass an Athena-backed runner; under Spark, pass ``lambda q: [r.asDict() for r in
spark.sql(q).collect()]``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

DB_RAW = "streaming_raw"
DB_PROCESSED = "streaming_processed"
DB_REPORTING = "streaming_reporting"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


# Each check: (name, sql, predicate over the first row).
CHECKS: list[tuple[str, str, Callable[[dict], bool]]] = [
    (
        "fact_playback_events == raw row count (no FK drops)",
        f"""SELECT
              (SELECT count(*) FROM {DB_PROCESSED}.fact_playback_events) AS fact_count,
              (SELECT count(*) FROM {DB_RAW}.playback_events) AS raw_count""",
        lambda r: int(r["fact_count"]) == int(r["raw_count"]),
    ),
    (
        "all fact_playback_events have title_key",
        f"""SELECT count(*) AS total, count(title_key) AS with_key
            FROM {DB_PROCESSED}.fact_playback_events""",
        lambda r: int(r["total"]) == int(r["with_key"]),
    ),
    (
        "all fact_playback_events have customer_key",
        f"""SELECT count(*) AS total, count(customer_key) AS with_key
            FROM {DB_PROCESSED}.fact_playback_events""",
        lambda r: int(r["total"]) == int(r["with_key"]),
    ),
    (
        "all fact_playback_events.date_key resolve in dim_date",
        f"""SELECT
              (SELECT count(*) FROM {DB_PROCESSED}.fact_playback_events) AS total,
              (SELECT count(*) FROM {DB_PROCESSED}.fact_playback_events e
                 JOIN {DB_PROCESSED}.dim_date d ON e.date_key = d.date_key) AS resolved""",
        lambda r: int(r["total"]) == int(r["resolved"]),
    ),
    (
        "date_key matches event_date (no off-by-one)",
        # year/month/day arithmetic is portable across Trino (Athena) and Spark;
        # date_format(_, 'yyyyMMdd') is Spark-only and breaks under Trino.
        f"""SELECT count(*) AS mismatches
            FROM {DB_PROCESSED}.fact_playback_events
            WHERE date_key <> (year(event_date) * 10000 + month(event_date) * 100 + day(event_date))""",
        lambda r: int(r["mismatches"]) == 0,
    ),
    (
        "fact_view_sessions count == distinct session_id in events",
        f"""SELECT
              (SELECT count(*) FROM {DB_PROCESSED}.fact_view_sessions) AS sessions,
              (SELECT count(DISTINCT session_id) FROM {DB_PROCESSED}.fact_playback_events) AS distinct_ids""",
        lambda r: int(r["sessions"]) == int(r["distinct_ids"]),
    ),
    (
        "all sessions have completion_pct in [0,1]",
        f"""SELECT count(*) AS total,
                   sum(CASE WHEN completion_pct BETWEEN 0 AND 1.001 THEN 1 ELSE 0 END) AS valid
            FROM {DB_PROCESSED}.fact_view_sessions""",
        lambda r: int(r["total"]) == int(r["valid"]),
    ),
    (
        "session_end_ts >= session_start_ts (no time travel)",
        f"""SELECT count(*) AS total,
                   sum(CASE WHEN session_end_ts >= session_start_ts THEN 1 ELSE 0 END) AS valid
            FROM {DB_PROCESSED}.fact_view_sessions""",
        lambda r: int(r["total"]) == int(r["valid"]),
    ),
    (
        "watch_duration <= session duration",
        # to_unixtime is the Trino (Athena) epoch fn; Spark's unix_timestamp is not
        # registered in Trino, so use to_unixtime for the daily_rollup Athena runner.
        f"""SELECT count(*) AS violations
            FROM {DB_PROCESSED}.fact_view_sessions
            WHERE watch_duration_seconds >
                  (to_unixtime(session_end_ts) - to_unixtime(session_start_ts)) + 1""",
        lambda r: int(r["violations"]) == 0,
    ),
    (
        "reporting sessions_count == processed sessions",
        f"""SELECT
              (SELECT sum(sessions_count) FROM {DB_REPORTING}.content_engagement_daily) AS reporting,
              (SELECT count(*) FROM {DB_PROCESSED}.fact_view_sessions) AS processed""",
        lambda r: int(r["reporting"]) == int(r["processed"]),
    ),
    (
        "device total watch == content total watch",
        f"""SELECT
              (SELECT sum(total_watch_seconds) FROM {DB_REPORTING}.device_engagement_daily) AS device_total,
              (SELECT sum(total_watch_seconds) FROM {DB_REPORTING}.content_engagement_daily) AS content_total""",
        lambda r: int(r["device_total"]) == int(r["content_total"]),
    ),
    (
        "genre_mix pct_of_day sums to ~100% per day",
        f"""SELECT engagement_date, sum(pct_of_day) AS total_pct
            FROM {DB_REPORTING}.genre_mix_daily GROUP BY engagement_date""",
        lambda r: 99.0 <= float(r["total_pct"]) <= 101.0,
    ),
    (
        "fact_daily_engagement.date_key resolves in dim_date",
        f"""SELECT
              (SELECT count(*) FROM {DB_PROCESSED}.fact_daily_engagement) AS total,
              (SELECT count(*) FROM {DB_PROCESSED}.fact_daily_engagement f
                 JOIN {DB_PROCESSED}.dim_date d ON f.date_key = d.date_key) AS resolved""",
        lambda r: int(r["total"]) == int(r["resolved"]),
    ),
]


def run_all(run_query: Callable[[str], list]) -> list[CheckResult]:
    """Execute all assertions using an injected ``run_query(sql) -> rows`` callable.

    Returns a CheckResult per check. Raises nothing — a query error is captured as
    a failed CheckResult so the caller can decide whether to block downstream.
    """
    results: list[CheckResult] = []
    for name, sql, predicate in CHECKS:
        try:
            rows = run_query(sql)
            if not rows:
                results.append(CheckResult(name, False, "no rows returned"))
                continue
            row = rows[0]
            ok = predicate(row)
            results.append(CheckResult(name, ok, "" if ok else f"row={dict(row)}"))
        except Exception as exc:  # noqa: BLE001 — surface as a failed check, don't crash the DAG task
            results.append(CheckResult(name, False, f"error: {exc}"))
    return results


def assert_all(run_query: Callable[[str], list]) -> None:
    """Run all checks and raise if any failed — convenient for an Airflow task."""
    results = run_all(run_query)
    failed = [r for r in results if not r.passed]
    for r in results:
        print(f"  [{'PASS' if r.passed else 'FAIL'}] {r.name} {r.detail}")
    if failed:
        raise AssertionError(f"{len(failed)}/{len(results)} reconciliation checks failed")
    print(f"PASS — all {len(results)} reconciliation checks succeeded")
