"""DAG: reporting_marts_dbt — the reporting marts, built by dbt via Cosmos.

Cosmos renders each model in the dbt project (dbt/streaming) and its tests as
individual Airflow tasks, running them with the isolated dbt-athena venv baked
into the image (DBT_EXECUTABLE_PATH). This is the dbt half of the hybrid: Spark/
Glue builds the heavy facts + dims (dbt sources); dbt builds the content / device
/ genre_mix / funnel marts in streaming_reporting. See dbt/streaming/README.md.

Parse strategy = LoadMode.CUSTOM: Cosmos reads the dbt .sql/.yml directly, so the
DAG renders with NO dbt binary, warehouse connection, or manifest at parse time
(keeps CI + the dag-processor happy). dbt only runs at task-execution time.

Manual (schedule=None) — trigger after a fact refresh, or wire onto RAW_EVENTS later.
"""

from __future__ import annotations

import os
from pathlib import Path

import pendulum
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# Container defaults; overridden by env in CI (repo checkout path) / compose.
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/streaming"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")
# 'dev' writes to streaming_reporting (the real marts); 'safe' -> streaming_reporting_dbt.
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

profile_config = ProfileConfig(
    profile_name="streaming",
    target_name=DBT_TARGET,
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

reporting_marts_dbt = DbtDag(
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    # CUSTOM = parse files directly (no dbt/warehouse/manifest needed at parse).
    render_config=RenderConfig(load_method=LoadMode.CUSTOM),
    # install_deps fetches dbt_utils into the venv before each dbt command at runtime.
    operator_args={"install_deps": True},
    dag_id="reporting_marts_dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    tags=["streaming", "dbt", "reporting"],
)
