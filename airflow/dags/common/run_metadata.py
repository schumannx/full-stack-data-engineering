"""Persist per-run DQ/ops metrics to an Iceberg table (streaming_ops.run_metadata)
so dq_scorecard can show trends + latency over time (not just the current state).

One row per numeric metric: (dag_id, run_id, metric_name, metric_value,
data_interval_start, data_interval_end, recorded_at). The schema/table are created
on first write (Athena CREATE ... IF NOT EXISTS) — so no Terraform dependency.
Needs Glue create/update + S3 write (the EC2 instance role and local admin creds
have it; the ECS role would need those perms added before it could write here).
"""

from __future__ import annotations

OPS_DB = "streaming_ops"
OPS_TABLE = "run_metadata"


def write_metrics(
    *,
    dag_id: str,
    run_id: str,
    di_start,
    di_end,
    metrics: dict,
    region: str,
    s3_bucket: str,
    results_loc: str,
    context_db: str = "streaming_raw",
) -> int:
    """Append numeric `metrics` for this run. Non-numeric values are skipped.
    Returns the number of rows written."""
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook

    rows = []
    for name, val in metrics.items():
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        rows.append(
            f"('{dag_id}','{run_id}','{name}',{v},"
            f"timestamp '{di_start}',timestamp '{di_end}',current_timestamp)"
        )
    if not rows:
        return 0

    hook = AthenaHook(aws_conn_id="aws_default", region_name=region)

    def run(sql: str):
        qid = hook.run_query(
            sql,
            query_context={"Database": context_db},  # any existing db for context
            result_configuration={"OutputLocation": results_loc},
        )
        hook.poll_query_status(qid)

    run(f"CREATE SCHEMA IF NOT EXISTS {OPS_DB}")
    run(
        f"""CREATE TABLE IF NOT EXISTS {OPS_DB}.{OPS_TABLE} (
              dag_id string, run_id string, metric_name string, metric_value double,
              data_interval_start timestamp, data_interval_end timestamp, recorded_at timestamp)
            LOCATION 's3://{s3_bucket}/{OPS_DB}.db/{OPS_TABLE}/'
            TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')"""
    )
    run(
        f"INSERT INTO {OPS_DB}.{OPS_TABLE} "
        "(dag_id, run_id, metric_name, metric_value, data_interval_start, data_interval_end, recorded_at) "
        f"VALUES {','.join(rows)}"
    )
    return len(rows)
