#!/usr/bin/env python3
"""Skill #03 — always-on Kafka → landing Iceberg consumer (systemd).

Single-instance micro-batch writer. Buffers consumed events and does ONE Iceberg
append per flush, where a flush fires on max(rows=FLUSH_MAX_ROWS, secs=
FLUSH_MAX_SECONDS). Offsets are committed only AFTER the Iceberg commit succeeds,
giving at-least-once delivery; duplicates are resolved by event_id dedup in the
raw zone (skill #04). See DESIGN.md §3.4 and PLAN_v2.md §3.

Guardrails (why single-instance, why manual commit): concurrent writers would
conflict on the landing table commit; manual post-append commit is what makes the
"no data loss on crash" guarantee hold.

Run locally:
  KAFKA_BOOTSTRAP=localhost:9092 python consumer.py
Run on EC2: via the systemd unit in systemd/streaming-consumer.service
"""

from __future__ import annotations

import signal
import time
from datetime import datetime, timezone

import pyarrow as pa
from kafka import KafkaConsumer

import config
from create_landing_table import get_catalog

_running = True


def _stop(signum, frame):
    global _running
    print(f"Received signal {signum}; will flush and exit after current batch")
    _running = False


def _to_record(msg) -> dict:
    """Normalize a Kafka message into a landing-table row.

    server_received_at is stamped from the broker message timestamp (ms since
    epoch); event_date/event_hour are derived from event_timestamp for partitioning.
    """
    evt = msg.value
    srv = datetime.fromtimestamp(msg.timestamp / 1000.0, tz=timezone.utc)
    ts_raw = evt.get("event_timestamp")
    try:
        et = datetime.fromisoformat(ts_raw) if ts_raw else srv
        if et.tzinfo is None:
            et = et.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        et = srv
    return {
        "event_id": evt.get("event_id"),
        "session_id": evt.get("session_id"),
        "customer_id": evt.get("customer_id"),
        "title_id": evt.get("title_id"),
        "device_id": evt.get("device_id"),
        "device_version_id": evt.get("device_version_id"),
        "event_type": evt.get("event_type"),
        "event_timestamp": et,
        "server_received_at": srv,
        "position_ms": evt.get("position_ms"),
        "bitrate_kbps": evt.get("bitrate_kbps"),
        "geo_country": evt.get("geo_country"),
        "schema_version": evt.get("schema_version", config.SCHEMA_VERSION),
        "event_date": et.strftime("%Y-%m-%d"),
        "event_hour": et.hour,
    }


def _flush(table, buffer: list[dict]) -> None:
    """Append the buffered rows to the landing Iceberg table in one commit."""
    if not buffer:
        return
    arrow = pa.Table.from_pylist(buffer, schema=table.schema().as_arrow())
    table.append(arrow)


def main():
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    catalog = get_catalog()
    table = catalog.load_table(config.TABLE_IDENTIFIER)

    consumer = KafkaConsumer(
        config.TOPIC,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,            # we commit AFTER the Iceberg append
        auto_offset_reset="earliest",
        value_deserializer=lambda b: __import__("json").loads(b.decode("utf-8")),
        max_poll_records=10000,
    )

    buffer: list[dict] = []
    last_flush = time.monotonic()
    print(f"Consuming '{config.TOPIC}' → {config.TABLE_IDENTIFIER} "
          f"(flush at {config.FLUSH_MAX_ROWS} rows / {config.FLUSH_MAX_SECONDS}s)")

    try:
        while _running:
            batch = consumer.poll(timeout_ms=1000)
            for _tp, messages in batch.items():
                for msg in messages:
                    buffer.append(_to_record(msg))

            age = time.monotonic() - last_flush
            if buffer and (len(buffer) >= config.FLUSH_MAX_ROWS or age >= config.FLUSH_MAX_SECONDS):
                _flush(table, buffer)
                consumer.commit()            # offsets advance only after commit
                print(f"  flushed {len(buffer)} rows; offsets committed")
                buffer.clear()
                last_flush = time.monotonic()
                table.refresh()
    finally:
        # Drain on shutdown so we don't lose the in-memory buffer.
        if buffer:
            _flush(table, buffer)
            consumer.commit()
            print(f"  final flush of {len(buffer)} rows on shutdown")
        consumer.close()
        print("Consumer stopped cleanly")


if __name__ == "__main__":
    main()
