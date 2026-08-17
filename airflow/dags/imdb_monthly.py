"""DAG: imdb_monthly — refresh the IMDb master, then load it into raw.

    lambda_mirror_imdb >> glue_imdb_to_raw   (emits Asset: IMDB_RAW)

The Lambda streams the gzipped TSVs from datasets.imdbws.com straight to S3
(multipart, no decompress); the Glue job converts them to Iceberg in raw.
See DESIGN.md §3.4 (skill #01) and PLAN_v2.md §2.
"""

from __future__ import annotations

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.sdk import dag

from common import config
from common.assets import IMDB_RAW


@dag(
    dag_id="imdb_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args=config.DEFAULT_ARGS,
    tags=["streaming", "imdb", "source"],
)
def imdb_monthly():
    mirror = LambdaInvokeFunctionOperator(
        task_id="lambda_mirror_imdb",
        function_name=config.LAMBDA_IMDB_MIRROR,
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
    )

    to_raw = GlueJobOperator(
        task_id="glue_imdb_to_raw",
        job_name=config.GLUE["imdb_to_raw"],
        aws_conn_id="aws_default",
        region_name=config.AWS_REGION,
        outlets=[IMDB_RAW],
    )

    mirror >> to_raw


imdb_monthly()
