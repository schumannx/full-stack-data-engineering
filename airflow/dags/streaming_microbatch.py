"""DAG: streaming_microbatch — the hot path, every 15 minutes.

    wait_landing(S3KeySensor)
        >> glue_raw_events
        >> dq_check                  (raw-zone GATE: quarantine% / volume — fails fast)
        >> glue_fact_playback_events
        >> glue_fact_view_sessions   (MERGE, 24h late window)
        >> write_run_metadata        (emits Asset: RAW_EVENTS)

Each Glue job receives ``data_interval_start`` so partitions/MERGE keys are
deterministic and re-runs are idempotent (DESIGN.md §4.5). The landing→raw step
is the only Airflow-owned part of ingestion; the Kafka consumer that fills
landing runs continuously under systemd, NOT here (DESIGN.md §4.1 scope boundary).
"""

from __future__ import annotations

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Variable, dag, task

from common import config
from common.assets import RAW_EVENTS

# Landing is Iceberg, partitioned by (event_date, event_hour) with NON-padded integer
# hours (event_hour=0..23). The raw job filters precisely by server_received_at, so the
# sensor only needs to gate on landing being non-empty — event-time partitions diverge
# from the logical run hour for backfills/late data, so an exact hour prefix is wrong.
LANDING_KEY = config.LANDING_EVENTS_PREFIX + "/event_date=*/event_hour=*/*.parquet"

# Args passed to every Glue job so the run is keyed to this interval. A manual
# trigger may override the window via dag_run.conf (data_interval_start/end) to
# backfill a historical range in one shot; scheduled runs use their 15-min slice.
GLUE_INTERVAL_ARGS = {
    "--data_interval_start": "{{ dag_run.conf.get('data_interval_start') or data_interval_start.isoformat() }}",
    "--data_interval_end": "{{ dag_run.conf.get('data_interval_end') or data_interval_end.isoformat() }}",
}


@dag(
    dag_id="streaming_microbatch",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "streaming", "hot-path"],
)
def streaming_microbatch():
    wait_landing = S3KeySensor(
        task_id="wait_landing",
        bucket_name=config.S3_BUCKET,
        bucket_key=LANDING_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        deferrable=True,  # frees the worker slot; resumes on the triggerer
        poke_interval=60,
        timeout=60 * 20,
    )

    raw_events = GlueJobOperator(
        task_id="glue_raw_events",
        job_name=config.GLUE["raw_events"],
        script_args=GLUE_INTERVAL_ARGS,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    fact_events = GlueJobOperator(
        task_id="glue_fact_playback_events",
        job_name=config.GLUE["fact_playback_events"],
        script_args=GLUE_INTERVAL_ARGS,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    fact_sessions = GlueJobOperator(
        task_id="glue_fact_view_sessions",
        job_name=config.GLUE["fact_view_sessions"],
        script_args=GLUE_INTERVAL_ARGS,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    @task
    def dq_check(**context):
        """Raw-zone quality GATE. Runs after raw_events, BEFORE the facts, so bad
        raw never propagates downstream (cheap fix, no multi-layer backfill). Fails
        the run if the batch is empty (ingestion drop) or the quarantine ratio is
        too high. Thresholds are Airflow Variables; window honours a conf override
        (same as the Glue jobs) so backfills check the right slice."""
        import pendulum as pdl
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        conf = context["dag_run"].conf or {}

        def to_ts(v):
            dt = v if hasattr(v, "in_timezone") else pdl.parse(v)
            return dt.in_timezone("UTC").format("YYYY-MM-DD HH:mm:ss")

        start = to_ts(conf.get("data_interval_start") or context["data_interval_start"])
        end = to_ts(conf.get("data_interval_end") or context["data_interval_end"])

        max_quarantine_pct = float(Variable.get("STREAMING_DQ_MAX_QUARANTINE_PCT", default_var="5.0"))
        min_rows = int(Variable.get("STREAMING_DQ_MIN_ROWS", default_var="1"))

        hook = AthenaHook(aws_conn_id="aws_default", region_name=config.AWS_REGION)
        results_loc = Variable.get(
            "STREAMING_ATHENA_RESULTS",
            default_var=f"s3://{config.S3_BUCKET}-use1-ops/athena-results/dq/",
        )

        def scalar(sql):
            qid = hook.run_query(
                sql,
                query_context={"Database": config.DB_RAW},
                result_configuration={"OutputLocation": results_loc},
            )
            hook.poll_query_status(qid)
            res = hook.get_query_results(qid)
            return int(res["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])

        window = f"server_received_at >= timestamp '{start}' AND server_received_at < timestamp '{end}'"
        raw_count = scalar(f"SELECT count(*) FROM playback_events WHERE {window}")
        try:
            quarantine_count = scalar(f"SELECT count(*) FROM playback_events_quarantine WHERE {window}")
        except Exception:
            quarantine_count = 0  # table only exists once something has been quarantined

        # Ingestion latency p99 (server_received_at - event_timestamp), for trends.
        lag_p99 = 0.0
        if raw_count:
            qid = hook.run_query(
                "SELECT coalesce(approx_percentile("
                "to_unixtime(server_received_at) - to_unixtime(event_timestamp), 0.99), 0) "
                f"FROM playback_events WHERE {window}",
                query_context={"Database": config.DB_RAW},
                result_configuration={"OutputLocation": results_loc},
            )
            hook.poll_query_status(qid)
            v = hook.get_query_results(qid)["ResultSet"]["Rows"][1]["Data"][0].get("VarCharValue")
            lag_p99 = round(float(v), 1) if v else 0.0

        total = raw_count + quarantine_count
        quarantine_pct = round(100.0 * quarantine_count / total, 3) if total else 0.0
        metrics = {"window_start": start, "window_end": end, "raw_count": raw_count,
                   "quarantine_count": quarantine_count, "quarantine_pct": quarantine_pct,
                   "lag_p99_seconds": lag_p99}
        print(f"[dq_check] {metrics}")

        failures = []
        if raw_count < min_rows:
            failures.append(f"raw_count {raw_count} < MIN_ROWS {min_rows} (ingestion drop?)")
        if quarantine_pct > max_quarantine_pct:
            failures.append(f"quarantine {quarantine_pct}% > MAX {max_quarantine_pct}%")
        if failures:
            raise AirflowException("DQ gate FAILED: " + "; ".join(failures))
        return metrics

    @task(outlets=[RAW_EVENTS])
    def write_run_metadata(dq_result=None, **context):
        """Persist the batch's DQ metrics to streaming_ops.run_metadata (trends +
        latency over time) and emit RAW_EVENTS so daily_rollup schedules on fresh
        data (DESIGN.md §4.5)."""
        from common.run_metadata import write_metrics

        if not dq_result:
            return dq_result
        results_loc = Variable.get(
            "STREAMING_ATHENA_RESULTS",
            default_var=f"s3://{config.S3_BUCKET}-use1-ops/athena-results/dq/",
        )
        write_metrics(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            di_start=dq_result.get("window_start"),
            di_end=dq_result.get("window_end"),
            metrics={k: dq_result[k] for k in
                     ("raw_count", "quarantine_count", "quarantine_pct", "lag_p99_seconds")
                     if k in dq_result},
            region=config.AWS_REGION,
            s3_bucket=config.S3_BUCKET,
            results_loc=results_loc,
        )
        return dq_result

    gate = dq_check()
    meta = write_run_metadata(gate)
    # Shift-left: gate sits between raw and the facts, so a bad batch blocks the
    # (expensive) fact builds instead of poisoning them.
    wait_landing >> raw_events >> gate >> fact_events >> fact_sessions >> meta


streaming_microbatch()
