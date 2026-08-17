"""DAG: daily_rollup — dims, daily fact, reporting aggregates, then serve.

    glue_dims_refresh
        >> glue_fact_daily_engagement
        >> glue_reporting_aggregates
        >> validate_etl (10 reconciliation assertions)
        >> lambda_render_html
        >> write_run_metadata

Data-aware schedule: fires on the RAW_EVENTS Asset produced by
streaming_microbatch AND on a 02:00 UTC cron, so it runs once fresh raw data
exists for the day (DESIGN.md §4.3). validate_etl reuses the compute-agnostic
assertions from common/validation.py (ported from streaming-etl).
"""

from __future__ import annotations

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.sdk import Variable, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from common import config
from common.assets import RAW_EVENTS

# A manual trigger may override the target day via dag_run.conf (engagement_date),
# e.g. to (re)build a historical day; scheduled runs use the run's logical date {{ ds }}.
GLUE_DAY_ARGS = {"--engagement_date": "{{ dag_run.conf.get('engagement_date') or ds }}"}


@dag(
    dag_id="daily_rollup",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 2 * * *", timezone="UTC"),
        assets=[RAW_EVENTS],
    ),
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "daily", "reporting"],
)
def daily_rollup():
    dims = GlueJobOperator(
        task_id="glue_dims_refresh",
        job_name=config.GLUE["dims_refresh"],
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    daily_fact = GlueJobOperator(
        task_id="glue_fact_daily_engagement",
        job_name=config.GLUE["fact_daily_engagement"],
        script_args=GLUE_DAY_ARGS,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    aggregates = GlueJobOperator(
        task_id="glue_reporting_aggregates",
        job_name=config.GLUE["reporting_aggregates"],
        script_args=GLUE_DAY_ARGS,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    @task
    def validate_etl(ds=None):
        """Run the cross-zone reconciliation assertions (common/validation.py) via
        Athena. A failure raises and blocks the reporting refresh (DESIGN.md §6.2)."""
        import sys

        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        sys.path.insert(0, "/opt/airflow/common")
        import validation  # the mounted ../common/validation.py

        hook = AthenaHook(aws_conn_id="aws_default", region_name=config.AWS_REGION)
        # Results go to the real us-east-1 ops bucket (<bucket>-use1-ops); the old
        # default pointed at a non-existent "-athena-results" bucket.
        results_loc = Variable.get(
            "STREAMING_ATHENA_RESULTS",
            default_var=f"s3://{config.S3_BUCKET}-use1-ops/athena-results/validate/",
        )

        def run_query(sql: str):
            # Athena requires a non-empty query context (catalog/database) on
            # StartQueryExecution even when the SQL is fully qualified, else
            # "queryExecutionContext.catalog and ...database are null or empty".
            qid = hook.run_query(
                sql,
                query_context={"Database": config.DB_PROCESSED},
                result_configuration={"OutputLocation": results_loc},
            )
            hook.poll_query_status(qid)
            res = hook.get_query_results(qid)
            rows = res["ResultSet"]["Rows"]
            header = [c["VarCharValue"] for c in rows[0]["Data"]]
            return [
                {header[i]: cell.get("VarCharValue") for i, cell in enumerate(r["Data"])}
                for r in rows[1:]
            ]

        validation.assert_all(run_query)
        return {"ds": ds, "assertions_passed": True}

    # NOTE: a redshift_refresh task lived here in the original design. The account is
    # on the AWS Free Plan (no Redshift), and Athena serves the same Iceberg/Spectrum
    # marts, so the serving layer is Athena-only — the task was removed (DESIGN.md §4.3,
    # cost decision). Reinstate a RedshiftDataOperator here if the plan is ever upgraded.
    render_html = LambdaInvokeFunctionOperator(
        task_id="lambda_render_html",
        function_name=config.LAMBDA_HTML_RENDER,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    @task
    def write_run_metadata(validation=None):
        return validation

    checked = validate_etl()
    dims >> daily_fact >> aggregates >> checked
    checked >> render_html >> write_run_metadata(checked)


daily_rollup()
