"""Lambda: streaming_html_render — render the reporting marts to an HTML dashboard.

Invoked by the ``daily_rollup`` DAG (task ``lambda_render_html``) after the
``streaming_reporting_aggregates`` Glue job has refreshed the day's marts. Queries
the four ``streaming_reporting`` Iceberg tables through Athena and writes a static
HTML page to S3 — the "BI" layer of the portfolio project (DESIGN.md §2.7).

Pure stdlib + boto3, so it ships as a plain zip with no layer. The heavy lifting
stays in Athena; this function only formats result rows into HTML.

Target day: ``event["engagement_date"]`` (``YYYY-MM-DD``) if supplied, else the
MAX(engagement_date) present in each table — so an ad-hoc invoke renders the
latest available data.
"""

from __future__ import annotations

import html
import os
import time

import boto3

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("STREAMING_S3_BUCKET", "acme-dw-streaming-xs2026")
DB_REPORTING = os.environ.get("STREAMING_REPORTING_DB", "streaming_reporting")
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT", f"s3://{S3_BUCKET}/athena-results/html_render/"
)
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
REPORTS_PREFIX = os.environ.get("REPORTS_PREFIX", "reports")

# table -> (title, columns to show, order-by clause, row limit).
# Columns must match streaming_reporting_aggregates' output (glue/jobs/reporting_aggregates.py).
PANELS = [
    ("content_engagement_daily", "Top Content by Watch Time",
     "primary_title, title_type, distinct_viewers, sessions_count, total_watch_seconds, completion_rate",
     "total_watch_seconds DESC", 20),
    ("device_engagement_daily", "Engagement by Device",
     "device_type, platform, sessions_count, distinct_viewers, total_watch_seconds, completion_rate",
     "total_watch_seconds DESC", 20),
    ("genre_mix_daily", "Genre Mix (share of watch time)",
     "genre, watch_seconds, pct_of_day", "watch_seconds DESC", 20),
    ("title_completion_funnel", "Title Completion Funnel (decile buckets)",
     "primary_title, title_type, bucket, sessions_in_bucket", "sessions_in_bucket DESC", 20),
]

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3")


def _run_query(sql: str) -> tuple[list[str], list[list[str]]]:
    """Run an Athena query to completion; return (header, rows)."""
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB_REPORTING},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        status = state["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    if status != "SUCCEEDED":
        reason = state.get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {qid} {status}: {reason}\nSQL: {sql}")

    header: list[str] = []
    rows: list[list[str]] = []
    paginator = athena.get_paginator("get_query_results")
    for i, page in enumerate(paginator.paginate(QueryExecutionId=qid)):
        result_rows = page["ResultSet"]["Rows"]
        for j, r in enumerate(result_rows):
            cells = [c.get("VarCharValue", "") for c in r["Data"]]
            if i == 0 and j == 0:  # first row of first page is the header
                header = cells
            else:
                rows.append(cells)
    return header, rows


def _resolve_date(table: str, requested: str | None) -> str | None:
    if requested:
        return requested
    _, rows = _run_query(f"SELECT max(engagement_date) FROM {table}")
    return rows[0][0] if rows and rows[0] and rows[0][0] else None


def _render_table(header: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "<p class='empty'>No rows.</p>"
    head = "".join(f"<th>{html.escape(h)}</th>" for h in header)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _build_html(day: str, panels: list[tuple[str, str]]) -> str:
    sections = "".join(
        f"<section><h2>{html.escape(title)}</h2>{table_html}</section>"
        for title, table_html in panels
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Streaming DW — Engagement Dashboard ({html.escape(day)})</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #222; }}
  h1 {{ font-weight: 600; }}
  section {{ margin: 2rem 0; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
  th {{ background: #b00710; color: #fff; }}
  tr:nth-child(even) td {{ background: #fafafa; }}
  .empty {{ color: #888; font-style: italic; }}
  footer {{ margin-top: 3rem; color: #888; font-size: 12px; }}
</style></head>
<body>
  <h1>Streaming DW — Engagement Dashboard</h1>
  <p>Reporting day: <strong>{html.escape(day)}</strong></p>
  {sections}
  <footer>Generated by streaming_html_render from {html.escape(DB_REPORTING)} via Athena.</footer>
</body></html>"""


def handler(event, context):
    event = event or {}
    requested = event.get("engagement_date")

    panels: list[tuple[str, str]] = []
    resolved_day = requested
    for table, title, cols, order_by, limit in PANELS:
        day = _resolve_date(table, requested)
        resolved_day = resolved_day or day
        if not day:
            panels.append((title, "<p class='empty'>No data.</p>"))
            continue
        header, rows = _run_query(
            f"SELECT {cols} FROM {table} "
            f"WHERE engagement_date = DATE '{day}' "
            f"ORDER BY {order_by} LIMIT {limit}"
        )
        panels.append((title, _render_table(header, rows)))

    day_label = resolved_day or "unknown"
    page = _build_html(day_label, panels)

    dated_key = f"{REPORTS_PREFIX}/dashboard_{day_label}.html"
    latest_key = f"{REPORTS_PREFIX}/latest.html"
    for key in (dated_key, latest_key):
        s3.put_object(
            Bucket=S3_BUCKET, Key=key, Body=page.encode("utf-8"),
            ContentType="text/html; charset=utf-8",
        )

    print(f"[html_render] wrote s3://{S3_BUCKET}/{dated_key} (and latest.html)")
    return {
        "status": "ok",
        "engagement_date": day_label,
        "s3_uri": f"s3://{S3_BUCKET}/{dated_key}",
        "latest_uri": f"s3://{S3_BUCKET}/{latest_key}",
    }


if __name__ == "__main__":
    print(handler({}, None))
