"""
Validator for generator output.

Asserts that emitted events conform to the schema and rules in:
  skills/streaming/02_synthetic_data_generator.md
  skills/streaming/03_source_streaming_events.md

Reads all *.jsonl.gz files under the given directory (recursively), prints PASS/FAIL.
"""

import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from mock_tconsts import MOCK_TITLES


REQUIRED_FIELDS = {
    "event_id": str,
    "session_id": str,
    "customer_id": str,
    "title_id": str,
    "device_id": str,
    "device_version_id": str,
    "event_type": str,
    "event_timestamp": str,
    "position_ms": int,
    "bitrate_kbps": int,
    "geo_country": str,
    "schema_version": int,
}

VALID_EVENT_TYPES = {"play", "pause", "seek", "resume", "complete", "exit"}
END_EVENT_TYPES = {"complete", "exit"}


def load_events(path: Path) -> list[dict]:
    files = sorted(path.glob("**/*.jsonl.gz"))
    if not files:
        print(f"FAIL: no .jsonl.gz files found under {path}")
        sys.exit(1)
    events = []
    for f in files:
        with gzip.open(f, "rt") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"FAIL: invalid JSON at {f}:{line_no}: {e}")
                    sys.exit(1)
    print(f"  loaded {len(events)} events from {len(files)} file(s)")
    return events


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def check_schema(events: list[dict]) -> list[str]:
    """Every event has all 12 required fields with correct types."""
    errors = []
    for i, e in enumerate(events):
        for field, expected_type in REQUIRED_FIELDS.items():
            if field not in e:
                errors.append(f"event #{i}: missing field '{field}'")
                continue
            if not isinstance(e[field], expected_type):
                errors.append(f"event #{i}: field '{field}' has type {type(e[field]).__name__}, expected {expected_type.__name__}")
    return errors


def check_enums(events: list[dict]) -> list[str]:
    errors = []
    for i, e in enumerate(events):
        if e.get("event_type") not in VALID_EVENT_TYPES:
            errors.append(f"event #{i}: invalid event_type '{e.get('event_type')}'")
        if e.get("schema_version") != 1:
            errors.append(f"event #{i}: schema_version != 1 (got {e.get('schema_version')})")
    return errors


def check_session_sequencing(events: list[dict]) -> list[str]:
    """For each session: order by ts, first must be 'play' at pos=0, last must be 'complete' or 'exit'."""
    errors = []
    by_session = defaultdict(list)
    for e in events:
        by_session[e["session_id"]].append(e)

    for sid, sess_events in by_session.items():
        sess_events.sort(key=lambda e: e["event_timestamp"])
        first, last = sess_events[0], sess_events[-1]
        if first["event_type"] != "play":
            errors.append(f"session {sid[:8]}: first event is '{first['event_type']}', expected 'play'")
        if first["position_ms"] != 0:
            errors.append(f"session {sid[:8]}: first 'play' has position_ms={first['position_ms']}, expected 0")
        if last["event_type"] not in END_EVENT_TYPES:
            errors.append(f"session {sid[:8]}: last event is '{last['event_type']}', expected complete/exit")
    return errors


def check_position_monotonicity(events: list[dict]) -> list[str]:
    """Within a session, position non-decreasing across non-seek events; seek can jump."""
    errors = []
    by_session = defaultdict(list)
    for e in events:
        by_session[e["session_id"]].append(e)

    for sid, sess_events in by_session.items():
        sess_events.sort(key=lambda e: e["event_timestamp"])
        last_pos = 0
        last_was_seek = False
        for e in sess_events:
            if e["event_type"] == "seek":
                last_pos = e["position_ms"]
                last_was_seek = True
                continue
            if last_was_seek:
                if e["position_ms"] < last_pos:
                    errors.append(f"session {sid[:8]}: position dropped from {last_pos} to {e['position_ms']} after seek")
                last_was_seek = False
            else:
                if e["position_ms"] < last_pos:
                    errors.append(f"session {sid[:8]}: position dropped from {last_pos} to {e['position_ms']} ({e['event_type']})")
            last_pos = e["position_ms"]
    return errors


def check_fk_integrity(events: list[dict], known_tconsts: set, known_customers: set) -> list[str]:
    errors = []
    for i, e in enumerate(events):
        if e["title_id"] not in known_tconsts:
            errors.append(f"event #{i}: title_id '{e['title_id']}' not in mock_tconsts")
        if e["customer_id"] not in known_customers:
            errors.append(f"event #{i}: customer_id '{e['customer_id']}' not in customer universe")
    return errors


def check_no_duplicate_event_ids(events: list[dict]) -> list[str]:
    seen = set()
    errors = []
    for i, e in enumerate(events):
        eid = e["event_id"]
        if eid in seen:
            errors.append(f"event #{i}: duplicate event_id '{eid}'")
        seen.add(eid)
    return errors


def check_distribution_sanity(events: list[dict]) -> list[str]:
    """Loose: peak-hour avg events should be > night-hour avg events. Skip if window misses both."""
    counts_by_hour = Counter()
    for e in events:
        counts_by_hour[parse_iso(e["event_timestamp"]).hour] += 1

    peak = sum(counts_by_hour[h] for h in range(19, 22))
    night = sum(counts_by_hour[h] for h in range(0, 6))
    peak_hours = sum(1 for h in range(19, 22) if counts_by_hour[h] > 0)
    night_hours = sum(1 for h in range(0, 6) if counts_by_hour[h] > 0)

    if peak_hours > 0 and night_hours > 0:
        peak_avg = peak / peak_hours
        night_avg = night / night_hours
        if peak_avg < night_avg:
            return [f"peak-hour avg ({peak_avg:.1f}) < night-hour avg ({night_avg:.1f}) - sinusoidal weights not respected"]
    return []


def derive_customer_universe(events: list[dict]) -> set:
    return {e["customer_id"] for e in events}


def print_stats(events: list[dict]) -> None:
    by_session = defaultdict(list)
    for e in events:
        by_session[e["session_id"]].append(e)

    sessions = list(by_session.values())
    n_completed = sum(1 for s in sessions if any(ev["event_type"] == "complete" for ev in s))
    n_exited = sum(1 for s in sessions if any(ev["event_type"] == "exit" for ev in s))

    event_type_counts = Counter(e["event_type"] for e in events)
    customers = {e["customer_id"] for e in events}
    titles = Counter(e["title_id"] for e in events)
    devices = Counter(e["device_id"] for e in events)

    print()
    print("Stats:")
    print(f"  events:                   {len(events)}")
    print(f"  sessions:                 {len(sessions)}")
    print(f"  active customers:         {len(customers)}")
    print(f"  distinct titles watched:  {len(titles)}")
    print(f"  distinct devices used:    {len(devices)}")
    if sessions:
        print(f"  completion rate:          {n_completed/len(sessions)*100:.1f}% ({n_completed}/{len(sessions)})")
        print(f"  exit rate:                {n_exited/len(sessions)*100:.1f}% ({n_exited}/{len(sessions)})")
    print(f"  events by type:           {dict(event_type_counts)}")


def main():
    p = argparse.ArgumentParser(description="Validate generator output against skill #02 + #03 spec")
    p.add_argument("path", help="Directory with *.jsonl.gz files (e.g. ./generator_out/)")
    args = p.parse_args()

    path = Path(args.path)
    if not path.exists():
        sys.exit(f"FAIL: path does not exist: {path}")

    print(f"Validating events in {path}/")
    events = load_events(path)
    if not events:
        sys.exit("FAIL: no events to validate")

    known_tconsts = {t["tconst"] for t in MOCK_TITLES}
    known_customers = derive_customer_universe(events)

    checks = [
        ("schema",                    lambda: check_schema(events)),
        ("enums",                     lambda: check_enums(events)),
        ("session sequencing",        lambda: check_session_sequencing(events)),
        ("position monotonicity",     lambda: check_position_monotonicity(events)),
        ("FK integrity",              lambda: check_fk_integrity(events, known_tconsts, known_customers)),
        ("no duplicate event_ids",    lambda: check_no_duplicate_event_ids(events)),
        ("distribution sanity",       lambda: check_distribution_sanity(events)),
    ]

    all_errors = []
    print()
    for name, fn in checks:
        errors = fn()
        if errors:
            print(f"  [FAIL] {name}: {len(errors)} error(s)")
            for err in errors[:5]:
                print(f"    - {err}")
            if len(errors) > 5:
                print(f"    ... and {len(errors) - 5} more")
            all_errors.extend(errors)
        else:
            print(f"  [PASS] {name}")

    print_stats(events)
    print()
    if all_errors:
        print(f"FAIL - {len(all_errors)} total errors")
        sys.exit(1)
    else:
        print("PASS - all checks succeeded")


if __name__ == "__main__":
    main()
