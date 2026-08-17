# Airflow — Streaming DW orchestration

Self-hosted Apache Airflow (**CeleryExecutor**, on Docker) that orchestrates the
**batch** half of the pipeline: IMDb load, landing→raw→processed→reporting Glue
jobs, Iceberg compaction, validation, and report serving.

> Airflow does **not** run the Kafka brokers or the always-on landing consumer —
> those are continuous systemd processes on EC2. Airflow only schedules things
> that start, finish, and have dependencies. See `../skills/streaming/DESIGN.md` §4
> and `../PLAN_v2.md`.

## Layout

| Path | What |
|---|---|
| `Dockerfile` | Custom image: base Airflow + `apache-airflow-providers-amazon`. Same image dev→prod (ECR). |
| `docker-compose.yaml` | Dev CeleryExecutor stack: scheduler, webserver, triggerer, worker, redis, postgres, init, (flower). |
| `requirements.txt` | Provider packages (resolved against Airflow constraints). |
| `.env.example` | Copy to `.env`; set `AIRFLOW_UID`, Fernet key, region, bucket. |
| `dags/common/config.py` | Shared constants: regions, bucket, Glue job names, Lambda names. |
| `dags/common/assets.py` | Datasets/Assets for data-aware cross-DAG scheduling. |
| `dags/imdb_monthly.py` | `@monthly`: Lambda mirror → Glue imdb→raw. |
| `dags/streaming_microbatch.py` | `*/15`: S3KeySensor → raw → facts → DQ → metadata (hot path). |
| `dags/daily_rollup.py` | Asset(RAW_EVENTS) + `0 2 * * *`: dims → daily fact → aggregates → validate → serve. |
| `dags/iceberg_maintenance.py` | `30 3 * * *`: compaction on `maintenance` queue. |
| `../common/validation.py` | The 10 reconciliation assertions, mounted read-only at `/opt/airflow/common`. |

The Glue job names in `config.py` and Lambda names are the **contract** with the
not-yet-built jobs under `glue/` and `lambda/`. The DAGs parse and schedule today;
tasks will succeed once those jobs exist.

## Run locally

```bash
cd airflow
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
# add a Fernet key to .env:
python -c "from cryptography.fernet import Fernet; print('AIRFLOW__CORE__FERNET_KEY='+Fernet.generate_key().decode())" >> .env

docker compose build
docker compose up airflow-init        # one-shot DB migrate + admin user
docker compose up -d
open http://localhost:8080            # login: airflow / airflow
```

Demo Celery scale-out (the reason we chose CeleryExecutor):

```bash
docker compose up -d --scale airflow-worker=3
docker compose --profile flower up -d flower   # http://localhost:5555
```

## Validate DAGs without the stack

```bash
# parse-check all DAGs in a throwaway container
docker compose run --rm airflow-scheduler airflow dags list
docker compose run --rm airflow-scheduler airflow dags test streaming_microbatch 2026-05-30
```

## Prod (apply-on-demand, not left running)

Prod swaps the `postgres`/`redis` containers for **RDS Postgres** + **ElastiCache
Redis** and runs the same image on **ECS Fargate**, defined under
`../infra/terraform/airflow-ecs/`. Default state is `terraform destroy`d; spin up
for a demo, then tear down (~$70–100/mo while applied). Prod uses the ECS **task
IAM role** for AWS access — no static keys, unlike the dev `~/.aws` mount.
