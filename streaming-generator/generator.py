"""
Streaming synthetic playback event generator.

Implements the happy-path spec from:
  skills/streaming/02_synthetic_data_generator.md

Scaled-down universe (100 customers, 20 device versions, 50 mock IMDb titles)
for fast local validation. Output is bit-identical for a given --seed.
"""

import argparse
import gzip
import json
import math
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from mock_tconsts import MOCK_TITLES, get_all_genres


def seeded_uuid(rng: np.random.Generator) -> str:
    """Generate a deterministic UUID from the seeded RNG (so same --seed = identical output)."""
    return str(uuid.UUID(bytes=bytes(rng.integers(0, 256, 16, dtype=np.uint8))))


# ─── Constants from skill #02 ──────────────────────────────────────────────────

ARCHETYPES = {
    "heavy":  {"pop_pct": 0.15, "lambda_per_week": 20, "session_mu": 2.5, "session_sigma": 0.6, "dirichlet_alpha": 0.3},
    "medium": {"pop_pct": 0.60, "lambda_per_week":  8, "session_mu": 2.0, "session_sigma": 0.5, "dirichlet_alpha": 0.5},
    "light":  {"pop_pct": 0.25, "lambda_per_week":  2, "session_mu": 1.5, "session_sigma": 0.4, "dirichlet_alpha": 0.8},
}

DEVICE_MIX = {
    "tv_primary":     {"pop_pct": 0.40, "primary": ["tv_roku", "tv_appletv", "tv_samsung"], "secondary": ["mobile_iphone"]},
    "mobile_primary": {"pop_pct": 0.30, "primary": ["mobile_iphone", "mobile_android"],     "secondary": ["laptop_chrome"]},
    "laptop_primary": {"pop_pct": 0.20, "primary": ["laptop_chrome", "laptop_safari", "laptop_firefox"], "secondary": ["mobile_iphone"]},
    "multi_device":   {"pop_pct": 0.10, "primary": ["__all__"],                              "secondary": []},
}

# Sinusoidal time-of-day weights — skill #02 lines 41-55
HOUR_WEIGHTS = {
    **{h: 0.05 for h in range(0, 6)},
    **{h: 0.15 for h in range(6, 8)},
    **{h: 0.20 for h in range(8, 12)},
    **{h: 0.25 for h in range(12, 17)},
    **{h: 0.50 for h in range(17, 19)},
    **{h: 1.00 for h in range(19, 22)},
    **{h: 0.40 for h in range(22, 24)},
}

DEVICE_MODELS = [
    {"id": "tv_roku",        "type": "tv",      "platform": "roku",        "model": "Roku-Ultra-2024"},
    {"id": "tv_appletv",     "type": "tv",      "platform": "appletv",     "model": "AppleTV-4K"},
    {"id": "tv_samsung",     "type": "tv",      "platform": "tizen",       "model": "Samsung-QN90"},
    {"id": "mobile_iphone",  "type": "mobile",  "platform": "ios",         "model": "iPhone-15-Pro"},
    {"id": "mobile_android", "type": "mobile",  "platform": "android",     "model": "Pixel-8"},
    {"id": "laptop_chrome",  "type": "laptop",  "platform": "web",         "model": "Chrome-Browser"},
    {"id": "laptop_safari",  "type": "laptop",  "platform": "web",         "model": "Safari-Browser"},
    {"id": "laptop_firefox", "type": "laptop",  "platform": "web",         "model": "Firefox-Browser"},
    {"id": "console_xbox",   "type": "console", "platform": "xbox",        "model": "Xbox-Series-X"},
    {"id": "console_ps",     "type": "console", "platform": "playstation", "model": "PS5"},
]

OS_VERSIONS = {
    "ios": ["17.4", "17.5"], "android": ["14.0", "15.0"], "tizen": ["7.0", "8.0"],
    "roku": ["13.0", "14.0"], "appletv": ["17.4", "17.5"], "web": ["122.0", "123.0"],
    "xbox": ["10.0", "10.1"], "playstation": ["8.0", "8.1"],
}

APP_VERSIONS = ["v8.5.2", "v8.6.1"]

COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX"]


# ─── Universe construction ─────────────────────────────────────────────────────

@dataclass
class Customer:
    customer_id: str
    archetype: str
    genre_affinity: dict
    device_mix: str
    primary_device_ids: list
    secondary_device_ids: list
    country: str


@dataclass
class DeviceVersion:
    device_id: str
    device_version_id: str
    device_type: str
    platform: str
    model: str
    app_version: str
    os_version: str


def build_customer_universe(n: int, rng: np.random.Generator) -> list[Customer]:
    all_genres = get_all_genres()
    arch_keys = list(ARCHETYPES)
    arch_probs = [ARCHETYPES[k]["pop_pct"] for k in arch_keys]
    dev_keys = list(DEVICE_MIX)
    dev_probs = [DEVICE_MIX[k]["pop_pct"] for k in dev_keys]

    customers = []
    for i in range(n):
        archetype = arch_keys[rng.choice(len(arch_keys), p=arch_probs)]
        alpha = ARCHETYPES[archetype]["dirichlet_alpha"]
        genre_weights = rng.dirichlet([alpha] * len(all_genres))
        genre_affinity = dict(zip(all_genres, genre_weights))

        dev_archetype = dev_keys[rng.choice(len(dev_keys), p=dev_probs)]
        dev_def = DEVICE_MIX[dev_archetype]
        if "__all__" in dev_def["primary"]:
            primary = [d["id"] for d in DEVICE_MODELS]
            secondary = []
        else:
            primary = list(dev_def["primary"])
            secondary = list(dev_def["secondary"])

        customers.append(Customer(
            customer_id=f"cust_{i:06d}",
            archetype=archetype,
            genre_affinity=genre_affinity,
            device_mix=dev_archetype,
            primary_device_ids=primary,
            secondary_device_ids=secondary,
            country=COUNTRIES[rng.choice(len(COUNTRIES))],
        ))
    return customers


def build_device_universe() -> list[DeviceVersion]:
    """10 device models × 2 versions = 20 device_versions."""
    versions = []
    for model in DEVICE_MODELS:
        os_list = OS_VERSIONS[model["platform"]]
        for i, (app_v, os_v) in enumerate(zip(APP_VERSIONS, os_list)):
            versions.append(DeviceVersion(
                device_id=model["id"],
                device_version_id=f"{model['id']}_v{i}",
                device_type=model["type"],
                platform=model["platform"],
                model=model["model"],
                app_version=app_v,
                os_version=os_v,
            ))
    return versions


# ─── Title and device selection ────────────────────────────────────────────────

def select_title(customer: Customer, rng: np.random.Generator):
    """Weighted random: genre_affinity × imdb_rating × log(num_votes)."""
    weights = []
    for title in MOCK_TITLES:
        title_genres = [g.strip() for g in title["genres"].split(",")]
        avg_affinity = sum(customer.genre_affinity.get(g, 0) for g in title_genres) / len(title_genres)
        score = avg_affinity * title["imdb_rating"] * math.log(title["num_votes"] + 1)
        weights.append(score)
    weights = np.array(weights)
    weights = weights / weights.sum()
    idx = rng.choice(len(MOCK_TITLES), p=weights)
    return MOCK_TITLES[idx]


def select_device(customer: Customer, devices: list[DeviceVersion], rng: np.random.Generator) -> DeviceVersion:
    """80% primary, 20% secondary. Multi-device customers pick uniformly."""
    if customer.device_mix == "multi_device":
        candidates = devices
    else:
        candidate_ids = customer.primary_device_ids if rng.random() < 0.8 or not customer.secondary_device_ids \
                        else customer.secondary_device_ids
        candidates = [d for d in devices if d.device_id in candidate_ids]
        if not candidates:
            candidates = devices
    return candidates[rng.choice(len(candidates))]


# ─── Session and event generation ──────────────────────────────────────────────

def generate_session_events(customer: Customer, device: DeviceVersion, title: dict,
                            session_start: datetime, rng: np.random.Generator) -> list[dict]:
    """Emit all events for one viewing session in chronological order."""
    session_id = seeded_uuid(rng)
    runtime_ms = title["runtime_minutes"] * 60 * 1000
    arch = ARCHETYPES[customer.archetype]

    # Watch duration (ms) — log-normal of session length, capped by runtime
    watch_seconds = float(rng.lognormal(arch["session_mu"], arch["session_sigma"])) * 3600
    watch_ms = min(int(watch_seconds * 1000), runtime_ms)
    watch_ms = max(watch_ms, 60_000)  # min 1 min

    is_series = title["title_type"] == "tvSeries"
    bitrate = int([1500, 3000, 5000, 8000][rng.choice(4)])

    n_pauses = min(int(rng.poisson(1.5 if is_series else 0.5)), 3)
    n_seeks  = min(int(rng.poisson(0.0 if is_series else 0.3)), 2)

    def make_event(event_type: str, ts: datetime, position_ms: int) -> dict:
        return {
            "event_id": seeded_uuid(rng),
            "session_id": session_id,
            "customer_id": customer.customer_id,
            "title_id": title["tconst"],
            "device_id": device.device_id,
            "device_version_id": device.device_version_id,
            "event_type": event_type,
            "event_timestamp": ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "position_ms": int(position_ms),
            "bitrate_kbps": bitrate,
            "geo_country": customer.country,
            "schema_version": 1,
        }

    events = []
    cur_ts = session_start
    cur_pos = 0
    events.append(make_event("play", cur_ts, cur_pos))

    # Split watch_ms into intervals, separated by pauses and seeks
    n_interruptions = n_pauses + n_seeks
    if n_interruptions == 0:
        intervals = [watch_ms]
    else:
        # Pick (n_interruptions) cut points uniformly in (0, watch_ms)
        cuts = sorted(rng.uniform(0, watch_ms, n_interruptions))
        boundaries = [0.0] + list(cuts) + [float(watch_ms)]
        intervals = [int(boundaries[i + 1] - boundaries[i]) for i in range(len(boundaries) - 1)]

    interruption_types = ["pause"] * n_pauses + ["seek"] * n_seeks
    rng.shuffle(interruption_types)

    for i, itype in enumerate(interruption_types):
        # Watch for intervals[i] before the interruption
        play_ms = intervals[i]
        cur_pos += play_ms
        cur_ts += timedelta(milliseconds=play_ms)

        if itype == "pause":
            events.append(make_event("pause", cur_ts, cur_pos))
            pause_dur_ms = int(rng.uniform(30_000, 300_000))
            cur_ts += timedelta(milliseconds=pause_dur_ms)
            events.append(make_event("resume", cur_ts, cur_pos))  # same position
        else:  # seek
            new_pos = int(rng.uniform(0, max(runtime_ms - 1000, 1)))
            events.append(make_event("seek", cur_ts, new_pos))
            cur_pos = new_pos

    # Final play interval — watch until session end
    final_ms = intervals[-1]
    cur_pos += final_ms
    cur_ts += timedelta(milliseconds=final_ms)

    completion_pct = cur_pos / runtime_ms
    end_event = "complete" if completion_pct >= 0.9 else "exit"
    events.append(make_event(end_event, cur_ts, cur_pos))

    return events


# ─── Session scheduling ────────────────────────────────────────────────────────

def distribute_sessions(customers: list[Customer], total_sessions: int,
                        rng: np.random.Generator) -> list[int]:
    """Distribute total_sessions across customers, weighted by archetype lambda."""
    weights = np.array([ARCHETYPES[c.archetype]["lambda_per_week"] for c in customers], dtype=float)
    weights = weights / weights.sum()
    return rng.multinomial(total_sessions, weights).tolist()


def schedule_session_starts(start_ts: datetime, end_ts: datetime, n_sessions: int,
                            rng: np.random.Generator) -> list[datetime]:
    """Place n_sessions starts in [start_ts, end_ts] weighted by HOUR_WEIGHTS."""
    if n_sessions == 0:
        return []
    hours = []
    cur = start_ts
    while cur < end_ts:
        weight = HOUR_WEIGHTS[cur.hour]
        if cur.weekday() >= 5 and 12 <= cur.hour <= 23:
            weight *= 1.5
        hours.append((cur, weight))
        cur += timedelta(hours=1)
    if not hours:
        return [start_ts] * n_sessions

    weights = np.array([w for _, w in hours], dtype=float)
    weights = weights / weights.sum()
    chosen_hours = rng.choice(len(hours), size=n_sessions, p=weights)
    starts = []
    for h_idx in chosen_hours:
        base = hours[h_idx][0]
        offset_s = float(rng.uniform(0, 3600))
        ts = base + timedelta(seconds=offset_s)
        if ts >= end_ts:
            ts = end_ts - timedelta(seconds=60)
        starts.append(ts)
    return starts


# ─── Output ────────────────────────────────────────────────────────────────────

def write_output_files(events: list[dict], out_dir: Path) -> dict:
    """Group events by hour partition (yyyy=*/mm=*/dd=*/hh=*) and write JSONL.gz files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    by_hour: dict = {}
    for e in events:
        ts = datetime.fromisoformat(e["event_timestamp"].replace("Z", "+00:00"))
        partition_key = ts.strftime("%Y%m%d_%H")
        by_hour.setdefault(partition_key, []).append(e)

    written = {}
    for partition_key, hour_events in sorted(by_hour.items()):
        ts = datetime.strptime(partition_key, "%Y%m%d_%H")
        partition_dir = out_dir / f"yyyy={ts.year}" / f"mm={ts.month:02d}" / f"dd={ts.day:02d}" / f"hh={ts.hour:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_file = partition_dir / f"events_{partition_key}.jsonl.gz"
        with gzip.open(out_file, "wt") as f:
            for e in hour_events:
                f.write(json.dumps(e) + "\n")
        written[str(out_file)] = len(hour_events)
    return written


# ─── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Streaming synthetic playback event generator (skill #02 happy path)")
    p.add_argument("--start-ts", required=True, help="ISO8601 start (e.g. 2026-05-02T20:00:00Z)")
    p.add_argument("--end-ts", required=True, help="ISO8601 end (e.g. 2026-05-02T21:00:00Z)")
    p.add_argument("--rate", type=float, default=1.0,
                   help="Target events per second (drives total session count). Default 1.0")
    p.add_argument("--imdb-titles", default=None, help="Ignored — uses mock_tconsts.MOCK_TITLES")
    p.add_argument("--customers-snapshot", default=None, help="Ignored — fresh universe each run")
    p.add_argument("--output-target", choices=["file", "s3"], default="file",
                   help="file = local dir, s3 = local then upload (use upload_to_s3.py separately)")
    p.add_argument("--kinesis-stream", default=None, help="Ignored in this minimal build")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    p.add_argument("--out-dir", default="./generator_out", help="Local output directory")
    p.add_argument("--n-customers", type=int, default=100, help="Customer universe size (scaled from 1M)")
    return p.parse_args()


AVG_EVENTS_PER_SESSION = 5  # rough estimate for converting --rate to session count


def main():
    args = parse_args()
    t0 = time.monotonic()

    rng = np.random.default_rng(args.seed)

    start_ts = datetime.fromisoformat(args.start_ts.replace("Z", "+00:00"))
    end_ts = datetime.fromisoformat(args.end_ts.replace("Z", "+00:00"))
    if end_ts <= start_ts:
        sys.exit("ERROR: --end-ts must be after --start-ts")

    duration_seconds = (end_ts - start_ts).total_seconds()
    target_events = int(args.rate * duration_seconds)
    target_sessions = max(target_events // AVG_EVENTS_PER_SESSION, 1)

    print(f"Generator config:")
    print(f"  window:           {start_ts} → {end_ts}  ({duration_seconds/3600:.2f}h)")
    print(f"  rate:             {args.rate} events/sec")
    print(f"  target events:    ~{target_events}")
    print(f"  target sessions:  ~{target_sessions}")
    print(f"  customers:        {args.n_customers}")
    print(f"  seed:             {args.seed}")
    print()

    customers = build_customer_universe(args.n_customers, rng)
    devices = build_device_universe()
    print(f"  built {len(customers)} customers, {len(devices)} device versions")

    sessions_per_customer = distribute_sessions(customers, target_sessions, rng)
    all_events = []
    for cust, n_sess in zip(customers, sessions_per_customer):
        if n_sess == 0:
            continue
        starts = schedule_session_starts(start_ts, end_ts, int(n_sess), rng)
        for session_start in starts:
            title = select_title(cust, rng)
            device = select_device(cust, devices, rng)
            all_events.extend(generate_session_events(cust, device, title, session_start, rng))

    all_events.sort(key=lambda e: e["event_timestamp"])

    out_dir = Path(args.out_dir)
    written = write_output_files(all_events, out_dir)

    runtime_seconds = round(time.monotonic() - t0, 3)
    metadata = {
        "events_emitted_count": len(all_events),
        "customers_active": len({e["customer_id"] for e in all_events}),
        "sessions_count": len({e["session_id"] for e in all_events}),
        "runtime_seconds": runtime_seconds,
        "seed_used": args.seed,
        "start_ts": args.start_ts,
        "end_ts": args.end_ts,
        "rate": args.rate,
        "n_customers": args.n_customers,
    }
    with (out_dir / "_run_metadata.json").open("w") as f:
        json.dump(metadata, f, indent=2)

    print()
    print(f"Wrote {len(written)} hour partitions to {out_dir}/")
    for path, n in sorted(written.items()):
        print(f"  {n:>5} events → {path}")
    print()
    print(f"Run summary: {len(all_events)} events, {metadata['sessions_count']} sessions, "
          f"{metadata['customers_active']} active customers, {runtime_seconds}s")


if __name__ == "__main__":
    main()
