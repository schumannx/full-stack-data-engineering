# Kafka ingest layer — Streaming DW

Self-managed **Kafka (KRaft, no ZooKeeper)** on EC2 plus a producer (skill #02
delivery) and an always-on **systemd consumer** (skill #03) that micro-batches
events into the **landing Iceberg** table.

> The Kafka **topic** (`streaming.playback_events`, 7-day retention) is the
> authoritative **replay/DR source** — not landing-S3, which is a mutable Iceberg
> table compacted by the `iceberg_maintenance` Airflow DAG. See
> `../skills/streaming/DESIGN.md` §2.4 / §3.4 and `../PLAN_v2.md` §3.

## Components

| Path | What |
|---|---|
| `config.py` | Env-overridable Kafka + Iceberg/Glue settings (bootstrap, topic, flush limits). |
| `producer.py` | Publish events to Kafka, keyed by `session_id`. Replay a file or generate in-process. |
| `consumer.py` | Always-on micro-batch consumer → landing Iceberg. At-least-once, manual offset commit. |
| `create_landing_table.py` | One-time pyiceberg DDL for the landing table (partitioned by event_date, event_hour). |
| `bootstrap/kraft_setup.sh` | Provision a single-node KRaft broker + systemd unit on EC2. |
| `bootstrap/create_topic.sh` | Create the topic with 6 partitions + 7-day retention. |
| `systemd/streaming-consumer.service` | systemd unit for the always-on consumer (single instance). |
| `requirements.txt` | kafka-python, pyiceberg[glue,s3fs], pyarrow, boto3. |

## Design guarantees

- **Micro-batch:** one Iceberg append per `max(rows=50_000, secs=60)` flush →
  ~1 file/partition/minute, avoiding the streaming small-file spiral.
- **At-least-once:** Kafka offsets commit **only after** the Iceberg append
  commits; a crash re-delivers the last batch, and `event_id` dedup in raw
  (skill #04) removes the duplicates.
- **Single writer:** one consumer instance only — concurrent writers would
  conflict on the landing table commit. Horizontal scale would require sharding
  by Kafka partition with a writer-per-shard and partition-disjoint commits.
- **Clean shutdown:** SIGTERM drains the in-memory buffer and commits before
  exit (`TimeoutStopSec=120`), so restarts lose no data.

## Local smoke test (laptop, local broker)

```bash
cd kafka
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. point at a local broker (e.g. `docker run -p 9092:9092 apache/kafka:3.9.0`)
export KAFKA_BOOTSTRAP=localhost:9092
export AWS_REGION=us-east-1 STREAMING_S3_BUCKET=acme-dw-streaming-xs2026

# 2. create the landing Iceberg table in Glue (one-time)
python create_landing_table.py

# 3. start the consumer in one shell
python consumer.py

# 4. publish events in another shell
python producer.py --generate --sessions 200
```

## EC2 deploy

```bash
# On the broker EC2:
sudo KAFKA_VERSION=3.9.0 bash bootstrap/kraft_setup.sh
bash bootstrap/create_topic.sh

# On the consumer EC2 (code under /opt/streaming/kafka, venv at .venv):
sudo cp systemd/streaming-consumer.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now streaming-consumer
journalctl -u streaming-consumer -f
```

The producer is launched ad-hoc via **SSM Run Command** (not always-on):

```bash
aws ssm send-command --document-name "AWS-RunShellScript" \
  --targets "Key=tag:role,Values=streaming-broker" \
  --parameters 'commands=["cd /opt/streaming/kafka && .venv/bin/python producer.py --generate --sessions 1000"]'
```
