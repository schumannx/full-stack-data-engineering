#!/usr/bin/env python3
"""Skill #02 delivery — publish generator events to Kafka.

Reuses the synthetic event shape from ../streaming-generator/generator.py
and publishes each event to the `streaming.playback_events` topic, **keyed by
session_id** so all events of a viewing session land on the same partition and
stay ordered. Launched ad-hoc on EC2 via SSM Run Command (DESIGN.md §3.4).

Two modes:
  --from-file events.json.gz   replay an existing generator dump
  --generate --sessions 200    generate fresh events in-process

Examples:
  KAFKA_BOOTSTRAP=broker:9092 python producer.py --generate --sessions 500
  python producer.py --from-file ../streaming-generator/generator_out/events.json.gz
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time

from kafka import KafkaProducer

import config


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",            # durability: wait for in-sync replicas
        linger_ms=50,          # small batching window
        retries=5,
    )


def _iter_from_file(path: str):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _iter_generated(sessions: int, customers: int, seed: int,
                    start_window_hours: float = 6.0, max_session_hours: float = 4.0):
    """Generate events in-process by importing the existing generator.

    Mirrors generator.main()'s universe construction, but streams session events
    to Kafka instead of writing hour-partitioned files. Uses the same numpy RNG so
    output is reproducible for a given seed.

    Session starts are placed in the window ``[now - max_session - start_window,
    now - max_session]`` so that even the longest title (~3.6h with pauses) finishes
    in the PAST. Otherwise events get future-dated relative to the consumer's
    ``server_received_at`` (= ingest time) and the raw-zone DQ gate quarantines them
    as ``future_client_clock`` (DESIGN.md §3.4).
    """
    sys.path.insert(0, "../streaming-generator")
    from datetime import datetime, timedelta, timezone

    import numpy as np

    import generator  # type: ignore

    rng = np.random.default_rng(seed)
    universe = generator.build_customer_universe(customers, rng)
    devices = generator.build_device_universe()
    window_s = start_window_hours * 3600.0
    base = datetime.now(timezone.utc) - timedelta(hours=max_session_hours) - timedelta(seconds=window_s)
    for _ in range(sessions):
        cust = universe[rng.choice(len(universe))]
        title = generator.select_title(cust, rng)
        device = generator.select_device(cust, devices, rng)
        offset = timedelta(seconds=float(rng.uniform(0, window_s)))
        yield from generator.generate_session_events(cust, device, title, base + offset, rng)


def publish(events, rate_per_sec: float | None) -> int:
    producer = _make_producer()
    n = 0
    interval = (1.0 / rate_per_sec) if rate_per_sec else 0.0
    try:
        for evt in events:
            evt.setdefault("schema_version", config.SCHEMA_VERSION)
            producer.send(config.TOPIC, key=evt.get("session_id"), value=evt)
            n += 1
            if n % 1000 == 0:
                print(f"  published {n} events")
            if interval:
                time.sleep(interval)
    finally:
        producer.flush()
        producer.close()
    return n


def main():
    p = argparse.ArgumentParser(description="Publish playback events to Kafka")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", help="Path to events.json[.gz] to replay")
    src.add_argument("--generate", action="store_true", help="Generate events in-process")
    p.add_argument("--sessions", type=int, default=200)
    p.add_argument("--customers", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--rate", type=float, default=None, help="Throttle to N events/sec")
    p.add_argument("--start-window-hours", type=float, default=6.0,
                   help="Width of the session-start window (events spread across it)")
    p.add_argument("--max-session-hours", type=float, default=4.0,
                   help="Safety gap so the longest session still finishes before now")
    args = p.parse_args()

    if args.from_file:
        events = _iter_from_file(args.from_file)
    else:
        events = _iter_generated(args.sessions, args.customers, args.seed,
                                 args.start_window_hours, args.max_session_hours)

    total = publish(events, args.rate)
    print(f"Published {total} events to topic '{config.TOPIC}'")


if __name__ == "__main__":
    main()
