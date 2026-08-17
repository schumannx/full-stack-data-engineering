"""daily_rollup with the marts layer run by Cosmos — the hybrid I'm recommending.

This is a real Python DAG (Airflow is always Python — Astronomer/Cosmos don't change
that). What changes: the four reporting marts that were four hand-wired GlueJobOperators
collapse into ONE DbtTaskGroup. Cosmos reads the dbt project, sees the `ref()` links,
and expands it into one Airflow task per model + per test — automatically.

Spark stays for the heavy `dims` job. Best of both: Spark signal + dbt/ELT signal,
one orchestration pane, no Astronomer platform, no extra cost.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

# Cosmos: the open-source dbt<->Airflow bridge (pip install astronomer-cosmos).
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import AthenaAccessKeyProfileMapping

from common import config
from common.assets import RAW_EVENTS

# Note: the data-aware DatasetOrTimeSchedule that dag-factory COULDN'T express is
# trivial here, because this is just Python.
PROFILE = ProfileConfig(
    profile_name="streaming",
    target_name="prod",
    profile_mapping=AthenaAccessKeyProfileMapping(
        conn_id="aws_default",
        profile_args={
            "region_name": config.AWS_REGION,
            "database": config.DB_REPORTING,
            "schema": config.DB_REPORTING,
            "s3_staging_dir": "s3://acme-dw-streaming-xs2026-use1-ops/athena-results/dbt/",
        },
    ),
)


@dag(
    dag_id="daily_rollup_cosmos",
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 2 * * *", timezone="UTC"),
        datasets=[RAW_EVENTS],
    ),
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "daily", "reporting", "dbt"],
)
def daily_rollup_cosmos():
    # Keep Spark for the heavy dimension refresh (SCD logic, big shuffles).
    dims = GlueJobOperator(
        task_id="glue_dims_refresh",
        job_name=config.GLUE["dims_refresh"],
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    # ONE line replaces fact_daily_engagement + the 4 reporting marts + validate_etl.
    # Cosmos renders each dbt model as a task, runs schema.yml tests as tasks, and
    # wires their dependencies from the ref() graph. No hand-wiring, no MERGE INTO.
    marts = DbtTaskGroup(
        group_id="dbt_marts",
        project_config=ProjectConfig("/opt/airflow/dbt/streaming"),
        profile_config=PROFILE,
        render_config=RenderConfig(select=["path:models/reporting"]),
        operator_args={"vars": '{"engagement_date": "{{ ds }}"}'},
    )

    dims >> marts


daily_rollup_cosmos()
