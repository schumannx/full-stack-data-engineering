# Blue/green for Airflow (shared data plane)

How we roll out a **new Airflow** (major version bump like 2→3, or a big code change)
without a risky in-place replacement — and roll back instantly if it misbehaves.

## The core rule (why Airflow blue/green is special)
Airflow is **stateful** (metadata DB, in-flight runs) and our **data plane is shared**
(one S3 Iceberg warehouse + Glue catalog + Kafka). So:

> **Both stacks RUN at once; only ONE is ACTIVE.**
> "Active" = its DAGs are **unpaused** (it triggers jobs that write the warehouse).
> If both were active they'd double-write the same Iceberg tables (the
> `ConcurrentRunsExceeded` / commit-conflict collisions we already hit with Glue).

- **blue**  = the currently-deployed Airflow (active, owns the warehouse)
- **green** = the new Airflow (running, DAGs paused, on the bench)

Each stack has its **own metadata DB** (separate per compose project) — correct, because
a major upgrade migrates the DB one-way. They **share** the AWS data plane (same `.env`).

```
        ┌─────────── blue (v current) ─ DAGs UNPAUSED = ACTIVE ───────┐
            scheduler/api/worker  ─┐
                                   ├──►  SHARED data plane
            scheduler/api/worker  ─┘     (S3 Iceberg, Glue catalog, Kafka)
        └─────────── green (v new) ─ DAGs PAUSED = STANDBY ───────────┘
   cutover = pause blue, unpause green   |   rollback = pause green, unpause blue
```

---

## Local demo (free, runs the pattern end-to-end)
Two compose projects on one machine, different host ports, **same shared AWS data plane**.
New DAGs come up **paused** (`DAGS_ARE_PAUSED_AT_CREATION=true`), so green is safe by default.

```bash
cd airflow

# 1. BLUE = current, active on :8080
AIRFLOW_HTTP_PORT=8080 docker compose -p streaming_blue up -d
#    (unpause its DAGs once — this is the "active" stack)
../deploy/blue-green/dags-set-state.sh streaming_blue unpause

# 2. GREEN = new build, on :8081, DAGs stay PAUSED
docker compose build                                   # build the new image
AIRFLOW_HTTP_PORT=8081 docker compose -p streaming_green up -d
#    UI: http://localhost:8081  (blue is http://localhost:8080)

# 3. VALIDATE green WITHOUT touching prod data — trigger into the scratch schema:
DBT_TARGET=safe AIRFLOW_HTTP_PORT=8081 docker compose -p streaming_green up -d
#    then trigger reporting_marts_dbt in green's UI (writes streaming_reporting_dbt)

# 4. CUTOVER (green becomes active, blue goes standby):
../deploy/blue-green/cutover.sh streaming_blue streaming_green

# 5. ROLLBACK if green misbehaves (instant — blue is still running):
../deploy/blue-green/cutover.sh streaming_green streaming_blue

# 6. DECOMMISSION blue once confident:
docker compose -p streaming_blue down            # add -v to drop its metadata volume
```

`cutover.sh` just pauses every DAG on the source stack, then unpauses them on the target
(see `dags-set-state.sh`). That pause/unpause **is** the blue/green switch for Airflow.

---

## Same thing on EC2 (prod)
The mechanism is identical — only the "where" changes:

| Local demo | EC2 prod |
|---|---|
| `docker compose -p streaming_blue` | EC2 instance **airflow-blue** (or one box, two compose projects) |
| `docker compose -p streaming_green` | EC2 instance **airflow-green** |
| different host ports | different instances / ports |
| same `.env` (shared AWS) | both use the EC2 task role → same Glue/S3/Kafka |
| run `cutover.sh` locally | run it via **SSM** on the box(es) |
| `docker compose down` | **stop** the blue EC2 (Free-Plan: stopped = ~$0, EBS only) |

Terraform manages the instances (manual `plan`/`apply` per your call — you can't
auto-validate infra + data + service together). Blue stays **stopped, not destroyed**, as
the rollback target until green is trusted.

---

## When NOT to use this
- **Minor changes** (new DAG, tweak a task): don't blue/green. Just deploy the DAG file to
  the active stack — Airflow's dag-processor re-parses live, no DB migration, no second stack.
  CI (`.github/workflows/ci.yml`) is the gate there.
- Blue/green is for **major version upgrades** and **risky code changes** where you want a
  parallel, validated stack and instant rollback.

## Beta → Prod is a *different* axis
Blue/green = two copies of **one** environment (upgrade + rollback).
Beta→Prod = **promotion** across environments (validate in beta, promote forward).
You can run blue/green *within* prod and *within* beta independently. Keep them separate in
your head: one is "safe upgrade," the other is "safe promotion."
