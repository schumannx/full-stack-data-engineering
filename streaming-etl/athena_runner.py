"""
Athena query runner — submits a SQL string, polls until completion, returns results.

Used by all run_skill_*.py scripts. Centralizes retry/polling/error handling.
"""

import sys
import time
from typing import Iterator

import boto3

import config


_athena = boto3.client("athena", region_name=config.ATHENA_REGION)


class AthenaQueryError(RuntimeError):
    pass


def run_query(sql: str, database: str | None = None,
              wait: bool = True, poll_interval: float = 1.0,
              timeout_seconds: float = 300.0) -> str:
    """
    Submit a SQL query, return the QueryExecutionId.
    If wait=True, blocks until the query succeeds or fails.
    Raises AthenaQueryError on failure.
    """
    params = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": config.ATHENA_RESULTS_LOCATION},
        "WorkGroup": config.ATHENA_WORKGROUP,
    }
    if database:
        params["QueryExecutionContext"] = {"Database": database}

    resp = _athena.start_query_execution(**params)
    qid = resp["QueryExecutionId"]

    if not wait:
        return qid

    return _wait(qid, poll_interval, timeout_seconds, sql)


def _wait(qid: str, poll_interval: float, timeout_seconds: float, sql: str) -> str:
    deadline = time.monotonic() + timeout_seconds
    while True:
        if time.monotonic() > deadline:
            raise AthenaQueryError(f"Query {qid} timed out after {timeout_seconds}s")
        info = _athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state == "SUCCEEDED":
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = info["Status"].get("StateChangeReason", "")
            short_sql = (sql[:200] + "...") if len(sql) > 200 else sql
            raise AthenaQueryError(f"Query {qid} {state}: {reason}\nSQL: {short_sql}")
        time.sleep(poll_interval)


def fetch_results(qid: str) -> Iterator[dict]:
    """Yield rows from a completed query as dicts {column: value}."""
    paginator = _athena.get_paginator("get_query_results")
    header = None
    for page in paginator.paginate(QueryExecutionId=qid):
        rows = page["ResultSet"]["Rows"]
        for row in rows:
            cells = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = cells
                continue
            yield dict(zip(header, cells))


def run_and_print(sql: str, label: str = "", database: str | None = None,
                  print_results: bool = True, max_rows: int = 20) -> str:
    """Convenience: run a query, print success + first few rows."""
    if label:
        print(f"  {label}", end=" ... ", flush=True)
    t0 = time.monotonic()
    qid = run_query(sql, database=database)
    elapsed = time.monotonic() - t0
    if label:
        print(f"OK ({elapsed:.1f}s)")
    if print_results:
        rows = list(fetch_results(qid))
        if rows:
            for r in rows[:max_rows]:
                print(f"    {r}")
            if len(rows) > max_rows:
                print(f"    ... and {len(rows) - max_rows} more rows")
    return qid


if __name__ == "__main__":
    qid = run_query("SELECT 1 AS smoke_test")
    print(f"Smoke test query {qid} succeeded")
    for row in fetch_results(qid):
        print(row)
