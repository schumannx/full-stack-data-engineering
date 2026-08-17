#!/usr/bin/env bash
# Create the playback events topic with 7-day retention (the replay/DR source).
# See DESIGN.md §2.4. Idempotent — Kafka errors are tolerated if it already exists.
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-streaming.playback_events}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"   # 7 days

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}" \
  --config "cleanup.policy=delete"

echo "Topic '${TOPIC}' ready: ${PARTITIONS} partitions, retention ${RETENTION_MS} ms"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
