"""DAG: dq_scorecard — daily data-quality summary, emailed via SNS.

Out-of-band auditor that complements the in-pipeline gate (streaming_microbatch
dq_check) and the dbt tests: once a day it queries the live tables and publishes
a scorecard (volume, quarantine %, orphan FKs, reporting freshness). For ad-hoc
deep dives, use analytics/duckdb/validate.sql.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Variable, dag, task

from common import config
from common.notify import sns_alert


@dag(
    dag_id="dq_scorecard",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "dq", "monitoring"],
)
def dq_scorecard():
    @task
    def build_and_send():
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        hook = AthenaHook(aws_conn_id="aws_default", region_name=config.AWS_REGION)
        results_loc = Variable.get(
            "STREAMING_ATHENA_RESULTS",
            default_var=f"s3://{config.S3_BUCKET}-use1-ops/athena-results/dq/",
        )

        def row(sql):
            qid = hook.run_query(
                sql,
                query_context={"Database": config.DB_RAW},
                result_configuration={"OutputLocation": results_loc},
            )
            hook.poll_query_status(qid)
            rows = hook.get_query_results(qid)["ResultSet"]["Rows"]
            return [c.get("VarCharValue") for c in rows[1]["Data"]]

        raw_24h = int(row(
            "SELECT count(*) FROM streaming_raw.playback_events "
            "WHERE server_received_at >= current_timestamp - interval '1' day")[0])
        try:
            q_24h = int(row(
                "SELECT count(*) FROM streaming_raw.playback_events_quarantine "
                "WHERE server_received_at >= current_timestamp - interval '1' day")[0])
        except Exception:
            q_24h = 0
        total = raw_24h + q_24h
        q_pct = round(100.0 * q_24h / total, 2) if total else 0.0

        orphan = row(
            "SELECT count_if(title_key IS NULL), count_if(device_key IS NULL) "
            "FROM streaming_processed.fact_playback_events")
        mart_day = row(
            "SELECT cast(max(engagement_date) AS varchar) "
            "FROM streaming_reporting.content_engagement_daily")[0]

        # Trends from persisted run_metadata (may not exist before the first write).
        try:
            avg_q7 = row(
                "SELECT round(avg(metric_value), 3) FROM streaming_ops.run_metadata "
                "WHERE metric_name = 'quarantine_pct' "
                "AND recorded_at >= current_timestamp - interval '7' day")[0]
            lat_p99 = row(
                "SELECT round(max(metric_value), 1) FROM streaming_ops.run_metadata "
                "WHERE metric_name = 'lag_p99_seconds' "
                "AND recorded_at >= current_timestamp - interval '1' day")[0]
        except Exception:
            avg_q7, lat_p99 = "n/a", "n/a"

        body = (
            "Streaming DW — Daily DQ scorecard\n"
            "================================\n"
            f"Raw events (24h):       {raw_24h}\n"
            f"Quarantined (24h):      {q_24h}  ({q_pct}%)\n"
            f"Quarantine% 7d avg:     {avg_q7}\n"
            f"Ingest latency p99 24h: {lat_p99}s\n"
            f"Orphan title FKs:       {orphan[0]}\n"
            f"Orphan device FKs:      {orphan[1]}\n"
            f"Latest reporting day:   {mart_day}\n"
        )
        print(body)
        sns_alert("[Airflow] Daily DQ scorecard", body)
        return {"raw_24h": raw_24h, "quarantine_pct": q_pct, "latest_mart_day": mart_day}

    build_and_send()


dq_scorecard()
