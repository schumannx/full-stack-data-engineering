"""
End-to-end validation across all 3 zones.

Asserts that data flows correctly: landing -> raw -> processed -> reporting,
with no silent data loss or row-count drift between layers.
"""

import sys

from athena_runner import run_query, fetch_results
import config


CHECKS = [
    {
        "name": "raw playback_events == landing JSON event count",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_RAW}.playback_events) AS raw_count,
              (SELECT count(*) FROM {config.DB_LANDING}.playback_events) AS landing_count
        """,
        "assert": lambda row: int(row["raw_count"]) <= int(row["landing_count"])
                              and int(row["raw_count"]) >= int(row["landing_count"]) * 0.95,
        "msg": "raw count should be within 5% of landing (allows for quarantine/dedup)",
    },
    {
        "name": "fact_playback_events == raw row count (no FK drops)",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_playback_events) AS fact_count,
              (SELECT count(*) FROM {config.DB_RAW}.playback_events) AS raw_count
        """,
        "assert": lambda row: int(row["fact_count"]) == int(row["raw_count"]),
    },
    {
        "name": "All fact_playback_events have title_key (no FK losses)",
        "sql": f"""
            SELECT
              count(*) AS total,
              count(title_key) AS with_title_key
            FROM {config.DB_PROCESSED}.fact_playback_events
        """,
        "assert": lambda row: int(row["total"]) == int(row["with_title_key"]),
    },
    {
        "name": "All fact_playback_events have customer_key",
        "sql": f"""
            SELECT
              count(*) AS total,
              count(customer_key) AS with_customer_key
            FROM {config.DB_PROCESSED}.fact_playback_events
        """,
        "assert": lambda row: int(row["total"]) == int(row["with_customer_key"]),
    },
    {
        "name": "All fact_playback_events.date_key resolve in dim_date",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_playback_events) AS total,
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_playback_events e
                 JOIN {config.DB_PROCESSED}.dim_date dd ON e.date_key = dd.date_key) AS resolved
        """,
        "assert": lambda row: int(row["total"]) == int(row["resolved"]),
    },
    {
        "name": "All fact_view_sessions.date_key resolve in dim_date",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS total,
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions s
                 JOIN {config.DB_PROCESSED}.dim_date dd ON s.date_key = dd.date_key) AS resolved
        """,
        "assert": lambda row: int(row["total"]) == int(row["resolved"]),
    },
    {
        "name": "All fact_daily_engagement.date_key resolve in dim_date",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_daily_engagement) AS total,
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_daily_engagement f
                 JOIN {config.DB_PROCESSED}.dim_date dd ON f.date_key = dd.date_key) AS resolved
        """,
        "assert": lambda row: int(row["total"]) == int(row["resolved"]),
    },
    {
        "name": "fact.date_key matches CAST(event_date) (no off-by-one)",
        "sql": f"""
            SELECT count(*) AS mismatches
            FROM {config.DB_PROCESSED}.fact_playback_events
            WHERE date_key <> CAST(date_format(event_date, '%Y%m%d') AS INTEGER)
        """,
        "assert": lambda row: int(row["mismatches"]) == 0,
    },
    {
        "name": "fact_view_sessions count == distinct session_id in events",
        "sql": f"""
            SELECT
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS sessions,
              (SELECT count(DISTINCT session_id) FROM {config.DB_PROCESSED}.fact_playback_events) AS distinct_session_ids
        """,
        "assert": lambda row: int(row["sessions"]) == int(row["distinct_session_ids"]),
    },
    {
        "name": "All sessions have valid completion_pct (0..1)",
        "sql": f"""
            SELECT
              count(*) AS total,
              count_if(completion_pct >= 0 AND completion_pct <= 1) AS valid
            FROM {config.DB_PROCESSED}.fact_view_sessions
        """,
        "assert": lambda row: int(row["total"]) == int(row["valid"]),
    },
    {
        "name": "session_end_ts >= session_start_ts (no time travel)",
        "sql": f"""
            SELECT
              count(*) AS total,
              count_if(session_end_ts >= session_start_ts) AS valid
            FROM {config.DB_PROCESSED}.fact_view_sessions
        """,
        "assert": lambda row: int(row["total"]) == int(row["valid"]),
    },
    {
        "name": "reporting sessions_count == processed sessions",
        "sql": f"""
            SELECT
              (SELECT sum(sessions_count) FROM {config.DB_REPORTING}.content_engagement_daily) AS reporting,
              (SELECT count(*) FROM {config.DB_PROCESSED}.fact_view_sessions) AS processed
        """,
        "assert": lambda row: int(row["reporting"]) == int(row["processed"]),
    },
    {
        "name": "device_engagement_daily total_watch_seconds == content_engagement_daily total",
        "sql": f"""
            SELECT
              (SELECT sum(total_watch_seconds) FROM {config.DB_REPORTING}.device_engagement_daily) AS device_total,
              (SELECT sum(total_watch_seconds) FROM {config.DB_REPORTING}.content_engagement_daily) AS content_total
        """,
        "assert": lambda row: int(row["device_total"]) == int(row["content_total"]),
    },
    {
        "name": "genre_mix_daily pct_of_day sums to ~100% per day",
        "sql": f"""
            SELECT engagement_date, sum(pct_of_day) AS total_pct
            FROM {config.DB_REPORTING}.genre_mix_daily
            GROUP BY engagement_date
        """,
        "assert": lambda row: 99.0 <= float(row["total_pct"]) <= 101.0,
    },
]


def main():
    print("=== End-to-End Validation ===")
    print()

    failures = []
    for chk in CHECKS:
        name = chk["name"]
        try:
            qid = run_query(chk["sql"])
            rows = list(fetch_results(qid))
            if not rows:
                print(f"  [WARN] {name}: no rows returned")
                continue
            for row in rows:
                if chk["assert"](row):
                    extra = chk.get("msg", "")
                    print(f"  [PASS] {name}  {dict(row)}")
                else:
                    print(f"  [FAIL] {name}  {dict(row)}")
                    failures.append((name, row))
        except Exception as e:
            print(f"  [ERROR] {name}: {e}")
            failures.append((name, str(e)))

    print()
    if failures:
        print(f"FAIL — {len(failures)} check(s) failed")
        for name, detail in failures:
            print(f"  - {name}: {detail}")
        sys.exit(1)
    else:
        print(f"PASS — all {len(CHECKS)} cross-zone reconciliation checks succeeded")


if __name__ == "__main__":
    main()
