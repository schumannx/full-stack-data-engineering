"""DAG: iceberg_maintenance — daily off-peak table upkeep.

    glue_compaction_landing >> glue_compaction_facts

Runs rewrite_data_files + expire_snapshots + rewrite_manifests against the
streaming-written tables to defeat the small-file problem (DESIGN.md §2.4, §5.2).
Routed to the dedicated `maintenance` Celery queue so heavy compaction does not
contend with the hot-path workers (DESIGN.md §4.2).
"""

from __future__ import annotations

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import dag

from common import config


@dag(
    dag_id="iceberg_maintenance",
    schedule="30 3 * * *",  # off-peak, after the 02:00 daily rollup
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "maintenance", "iceberg"],
)
def iceberg_maintenance():
    compact_landing = GlueJobOperator(
        task_id="glue_compaction_landing",
        job_name=config.GLUE["compaction_landing"],
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
        queue="maintenance",
    )

    compact_facts = GlueJobOperator(
        task_id="glue_compaction_facts",
        job_name=config.GLUE["compaction_facts"],
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
        queue="maintenance",
    )

    compact_landing >> compact_facts


iceberg_maintenance()
