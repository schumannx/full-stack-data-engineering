# Custom Airflow image — same image used in dev (Compose) and prod (ECS Fargate → ECR).
# Pins the amazon provider so GlueJobOperator / LambdaInvokeFunctionOperator /
# RedshiftDataOperator / AthenaOperator / S3KeySensor are available.
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Build-time pin so pip resolves against the matching Airflow constraints file.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt runs in an isolated venv so its deps (jinja2/click/protobuf/...) don't clash
# with Airflow's. Cosmos (in the Airflow env) invokes this binary via DBT_EXECUTABLE_PATH.
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-athena-community"
