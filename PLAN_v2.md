# Streaming DW — Revised Plan v2 (Kafka / Glue / Redshift / Airflow)

**Status:** Finalized 2026-05-30 · **Author:** schumannx
**Goal:** Portfolio skill-breadth. Self-managed Kafka, Glue PySpark, Redshift, and
self-hosted Airflow are all chosen deliberately to demonstrate breadth, accepting
higher cost than the original serverless (Firehose + Athena) design.

> This supersedes the compute/ingest layer of `skills/streaming/DESIGN.md`.
> The Kimball data model (§3 of DESIGN.md) is unchanged. DESIGN.md §4 (LeastAction)
> is retired and replaced by Airflow (this doc, §5).

---

## 1. End-to-end architecture

```
imdbws.com ──Lambda(stream gz)──▶ imdb_base/ (S3, TSV.gz)
                                       │
generator.py ─EC2 producer(SSM)─▶ Kafka topic streaming.playback_events
                                       │   (KRaft, retention ≥7d = REPLAY SOURCE)
                          EC2 consumer (systemd, always-on, micro-batch)
                                       ▼
                                  landing/  (Iceberg, hour-partitioned)
                                       ▼ Glue PySpark
                                    raw/     (Iceberg: dedup, cast, quarantine)
                                       ▼ Glue PySpark + Iceberg-Spark
                                 processed/  (Kimball: 8 dims + 3 facts, MERGE)
                                       ▼ Glue PySpark
                                 reporting/  (Iceberg pre-aggregates)
                                       ▼
                          Redshift Serverless (Spectrum external schema) + Lambda HTML

         ┌──────────────────────────────────────────────────────────┐
         │  Apache Airflow (self-host, CeleryExecutor, on Docker)     │
         │  orchestrates ONLY the batch boxes — NOT Kafka/consumer    │
         └──────────────────────────────────────────────────────────┘
```

## 2. Compute / storage / orchestration table

| Process | Compute | Storage / Format | Orchestrated by |
|---|---|---|---|
| imdbws → imdb_base | Lambda (stream `.gz` to S3, no decompress) | S3 TSV.gz | Airflow `imdb_monthly` |
| imdb_base → raw (IMDb) | Glue PySpark | Iceberg | Airflow `imdb_monthly` |
| generator → Kafka | EC2 Python producer via SSM | Kafka topic (KRaft) | manual / SSM (not Airflow) |
| Kafka → landing | EC2 Python consumer (systemd, micro-batch) | Iceberg, hour-partitioned | **not Airflow** (continuous) |
| landing → raw (events) | Glue PySpark G.1X, 4–10 DPU | Iceberg | Airflow `streaming_microbatch` (*/15) |
| landing → raw (snapshots) | Glue PySpark | Iceberg | Airflow `daily_rollup` |
| raw → processed (dims) | Glue PySpark + Iceberg-Spark | Iceberg | Airflow `daily_rollup` |
| raw → processed (facts) | Glue PySpark + Iceberg-Spark (MERGE) | Iceberg | Airflow `streaming_microbatch` + `daily_rollup` |
| processed → reporting | Glue PySpark on Iceberg | Iceberg v2 pre-agg | Airflow `daily_rollup` |
| Iceberg compaction | Glue (`rewrite_data_files`, `expire_snapshots`) | Iceberg | Airflow `iceberg_maintenance` |
| reporting → BI / Email | ~~Redshift Spectrum~~ + Lambda HTML | HTML on S3 | Airflow `daily_rollup` |

## 3. Ingest layer

- **Kafka:** KRaft (no ZooKeeper) on EC2. Topic `streaming.playback_events`, ~6 partitions,
  `retention.ms ≥ 7 days` — the replay/DR source now that landing is mutable Iceberg.
- **Producer:** existing `streaming-generator/generator.py` adapted to publish to Kafka;
  launched ad-hoc via **SSM Run Command** (not always-on, not Airflow).
- **Consumer (guardrails):**
  - `kafka-python`, **single instance** (systemd) → no concurrent Iceberg commit conflicts.
  - **Micro-batch flush:** buffer until `max(rows=50_000, seconds=60)`, then **one Iceberg append per flush** (~1 file/partition/min, avoids small-file spiral).
  - **At-least-once:** commit Kafka offsets only after Iceberg commit; dedup on `event_id` in raw.
  - Writes `landing/streaming/playback_events/` Iceberg, partitioned by `event_date, event_hour`.
- *Breadth alternative (deferred):* Spark Structured Streaming from Kafka instead of hand-rolled consumer.

## 4. Lake zones (deltas vs DESIGN.md)

- **landing/** is now **Iceberg** (was JSON.gz) → needs `iceberg_maintenance` compaction; replay = Kafka.
- **raw / processed / reporting** design unchanged, but **transforms rewritten Athena CTAS → Glue PySpark + Iceberg-Spark** (port `run_skill_04/05/06.py`).
- Kimball model (8 dims + 3 fact grains, MERGE-based session fact) kept as-is.
- `validate_etl.py`'s 10 reconciliation assertions → shared `common/validation.py`, called as an Airflow gating task.

## 5. Airflow orchestration (self-host, CeleryExecutor, on Docker)

### 5.1 Runtime — Docker everywhere; orchestrator differs by env

| | Dev (built & run for real) | Prod (committed, apply-on-demand, NOT left running) |
|---|---|---|
| Runs containers | Docker Compose (one host) | ECS Fargate |
| Image | custom `airflow/Dockerfile` | same image → ECR |
| Broker | `redis` container | ElastiCache Redis |
| Metadata + result backend | `postgres` container | RDS Postgres |
| Cost | $0 | ~$70–100/mo while up → destroy after demo |

### 5.2 CeleryExecutor topology (dev compose services)
`scheduler · webserver(:8080) · triggerer · worker · redis · postgres · airflow-init · flower(:5555, optional)`
- `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`, broker `redis://redis:6379/0`, result backend `db+postgresql://…`.
- Demo scale-out: `docker compose up --scale airflow-worker=3`.
- **Queue routing:** `default` = pipeline tasks; `maintenance` = Iceberg compaction (independent scaling).

### 5.3 The 4 DAGs

> **Superseded by the implementation — see README.md § Orchestration for the chains
> as built.** Two things changed: `dq_check` moved *ahead* of the fact jobs so bad
> raw data fails before it reaches the dimensional model, and `redshift_refresh` was
> dropped (Free-Plan account; Athena serves the same Iceberg tables). Two DAGs were
> also added since: `reporting_marts_dbt` and `dq_scorecard`.

```python
# imdb_monthly (@monthly)
lambda_mirror_imdb >> glue_imdb_to_raw                         # emits Asset: imdb_raw

# streaming_microbatch ("*/15 * * * *", catchup=False)
wait_landing(S3KeySensor) >> glue_raw_events \
    >> glue_fact_playback_events \
    >> glue_fact_view_sessions(MERGE, 24h window) \
    >> dq_check >> write_run_metadata                          # emits Asset: raw_events

# daily_rollup (schedule=[Asset(raw_events)] + "0 2 * * *")
glue_dims_refresh >> glue_fact_daily_engagement \
    >> glue_reporting_aggregates >> validate_etl(10 assertions) \
    >> [redshift_refresh, lambda_render_html] >> write_run_metadata

# iceberg_maintenance (daily off-peak, queue="maintenance")
glue_compaction_landing >> glue_compaction_facts
```

### 5.4 Operators / connections
S3KeySensor (deferrable) · GlueJobOperator + GlueJobSensor · LambdaInvokeFunctionOperator ·
RedshiftDataOperator · AthenaOperator · PythonOperator (validation) · Assets for cross-DAG triggers.
Connections: `aws_default` (dev = mounted `~/.aws`; prod = task IAM role, no static keys), `redshift_default`.
Bucket names / DPU counts as Airflow **Variables**.

### 5.5 Idempotency / observability
- Tasks keyed by `data_interval_start` → deterministic partitions/MERGE → clean `catchup` backfill.
- **`run_metadata`** table (rows in/out, dedup ratio, quarantine %, lag p99), Athena-queryable — replaces LeastAction catalog. Lineage = DAG edges + Assets.
- Alerting: SLA misses + `on_failure_callback` → SNS/Slack.

## 6. IAM
- `airflow_execution_role` (ECS task / dev local creds): glue StartJobRun/GetJobRun, lambda InvokeFunction, redshift-data:*, athena:*, S3 on bucket, CloudWatch logs.
- Per-zone Glue job roles (`role-streaming-raw-writer`, …) unchanged — each Glue job assumes only its own.
- Kafka EC2 instance profile: SSM + S3 (producer read / consumer Iceberg write).

## 7. Cost
- Data plane ~$8/day (DESIGN.md §5.2) — that total already includes the always-on Kafka
  and consumer EC2, so they are not additional.
- Airflow: dev $0; prod ~$70–100/mo only while ECS stack is applied (default = destroyed).

## 8. Build sequence

| Phase | Deliverable |
|---|---|
| 0 — Infra | Terraform: S3+lifecycle, Glue DBs, IAM, Kafka EC2, Redshift Serverless; (airflow-ecs written, not applied) |
| 1 — Ingest | Kafka KRaft; generator→Kafka producer (SSM); systemd consumer→landing Iceberg (micro-batch); compaction job |
| 2 — Transforms | Port `run_skill_04/05/06` → Glue PySpark; shared `common/validation.py` |
| 3 — Airflow | Dockerfile + docker-compose (Celery) dev; 4 DAGs; connections/Variables; `run_metadata` |
| 4 — Serving | Redshift Serverless external schema + report SQL; HTML-render Lambda |
| 5 — Prod (on-demand) | `terraform apply` airflow-ecs for demo → destroy; SLAs/alerts; backfill test |

## 9. Repo changes

```
NEW  infra/terraform/                  # AWS resources
NEW  infra/terraform/airflow-ecs/      # prod Airflow (committed, apply-on-demand)
NEW  kafka/                            # broker bootstrap, producer, consumer, *.service units
NEW  glue/                             # pyspark jobs (ported from streaming-etl)
NEW  airflow/                          # Dockerfile, docker-compose.yaml, dags/, plugins/, requirements.txt
NEW  lambda/                           # imdb_mirror/, html_render/
NEW  common/validation.py              # 10 assertions, callable from Airflow
MOD  skills/streaming/DESIGN.md  # §4 → Airflow; §2.7 stack row; §2.4 landing/replay; +compaction
KEEP streaming-generator/        # adapt to Kafka
ARCHIVE streaming-etl/           # Athena impl → reference baseline for validation
```

## 10. Next action
Rewrite `DESIGN.md` (§4 → Airflow, patch §2.7/§2.4 + compaction) so the canonical doc matches
this plan, then scaffold `airflow/` as the first code.
