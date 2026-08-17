"""
data.py — Seeded pandas-vectorized generator for streaming-themed entities
========================================================================

Produces four DataFrames matching the schema of the existing
streaming-generator project:

    customers   ~20 rows : customer_id, country, plan_type, signup_date
    titles      ~15 rows : title_id, genre, runtime_min
    devices     ~30 rows : device_id, device_type, customer_id
    events     ~200 rows : event_id, customer_id, title_id, device_id,
                           event_time, event_type, position_sec, watch_seconds

The generation is **seeded** (default seed=42) so output is fully reproducible.

Almost all of the work is vectorized using numpy + pandas primitives —
`np.repeat` to expand grouped entities (customers → sessions → events),
`groupby().cumsum()` to compute within-group running offsets, `rng.choice`
for categorical draws. The one small Python comprehension is the per-session
device pick, which is awkward to vectorize given variable device counts.

Generation is deliberately tuned so drill patterns appear naturally:

- `watch_seconds` is drawn from a 7-value bucket → ties show up naturally
  in any small sample (drill 02 — RANK vs DENSE_RANK).
- Sessions cluster events 1–14 min apart; new sessions for the same
  customer happen at uniformly random times across the 5-day window, so
  inter-session gaps are hours long (drill 15 — sessionization).
- Customers frequently re-watch the same title across sessions, so
  (customer_id, title_id) keys often have multiple rows (drill 16 — dedup).
- ~5 days of activity → real DAU series for drill 20.
- Master data is clean (no nulls); drill 17 injects its own nulls in a
  small slice so the demo is self-explanatory.

Run directly to inspect the four DataFrames:
    python data.py
"""

import numpy as np
import pandas as pd


SEED_DEFAULT = 42


def _make_customers(rng, n=20):
    return pd.DataFrame({
        "customer_id": [f"c{i:03d}" for i in range(1, n + 1)],
        "country":     rng.choice(["US","UK","JP","BR","DE"], n, p=[0.40, 0.20, 0.15, 0.15, 0.10]),
        "plan_type":   rng.choice(["basic","standard","premium"], n, p=[0.30, 0.40, 0.30]),
        "signup_date": (
            pd.Timestamp("2025-01-01")
            + pd.to_timedelta(rng.integers(0, 365, n), unit="D")
        ),
    })


def _make_titles(rng, n=15):
    return pd.DataFrame({
        "title_id":    [f"t{i:03d}" for i in range(1, n + 1)],
        "genre":       rng.choice(
            ["drama","comedy","action","horror","scifi"], n,
            p=[0.30, 0.30, 0.15, 0.10, 0.15],
        ),
        "runtime_min": rng.integers(30, 180, n).astype(int),
    })


def _make_devices(rng, customers):
    """Each customer owns 1–2 devices. Vectorized via np.repeat."""
    n_per = rng.integers(1, 3, len(customers))
    repeated_customer = np.repeat(customers["customer_id"].values, n_per)
    n = len(repeated_customer)
    return pd.DataFrame({
        "device_id":   [f"d{i:03d}" for i in range(1, n + 1)],
        "device_type": rng.choice(["tv","mobile","tablet","laptop"], n, p=[0.40, 0.30, 0.15, 0.15]),
        "customer_id": repeated_customer,
    })


def _make_events(rng, customers, titles, devices, n_days=5):
    """
    Expand: customer → 2–4 sessions → 2–5 events per session. Almost fully
    vectorized — the only loop is the per-session device pick.
    """
    customer_ids = customers["customer_id"].values
    title_ids    = titles["title_id"].values

    # 2–4 sessions per customer; np.repeat expands customer ids to session-level
    n_sessions_per = rng.integers(2, 5, len(customer_ids))
    session_owner = np.repeat(customer_ids, n_sessions_per)
    n_sessions = len(session_owner)

    base = pd.Timestamp("2026-05-01")
    session_starts = base + pd.to_timedelta(
        rng.integers(0, n_days * 24 * 3600, n_sessions), unit="s"
    )

    session_title = rng.choice(title_ids, n_sessions)

    # Pick one of the customer's devices per session (variable device counts → small loop)
    customer_devices = devices.groupby("customer_id")["device_id"].apply(list).to_dict()
    session_device = np.array([
        rng.choice(customer_devices[c]) for c in session_owner
    ])

    # 2–5 events per session; expand to event level
    n_events_per_session = rng.integers(2, 6, n_sessions)
    sess_idx = np.repeat(np.arange(n_sessions), n_events_per_session)
    n_events = len(sess_idx)

    is_first = np.concatenate([[True], sess_idx[1:] != sess_idx[:-1]])
    is_last  = np.concatenate([sess_idx[:-1] != sess_idx[1:], [True]])

    # Within-session minute offset: 0 on first event, otherwise 1–14 min gap, cumsum'd within session
    raw_gaps = rng.integers(1, 15, n_events).astype(int)
    raw_gaps[is_first] = 0
    within_min = pd.Series(raw_gaps).groupby(pd.Series(sess_idx)).cumsum().values

    # Position seconds: cumulative within session
    pos_inc = rng.integers(10, 200, n_events).astype(int)
    pos_inc[is_first] = 0
    position_sec = pd.Series(pos_inc).groupby(pd.Series(sess_idx)).cumsum().values

    # Event type: first=start, last=stop, middle picks pause/resume
    middle_type = rng.choice(["pause","resume"], n_events)
    event_type  = np.where(is_first, "start", np.where(is_last, "stop", middle_type))

    # Watch seconds: low-cardinality bucket so ties appear naturally
    watch_buckets = np.array([30, 60, 90, 120, 180, 240, 300])
    watch_seconds = rng.choice(watch_buckets, n_events)

    events = (
        pd.DataFrame({
            "customer_id":   session_owner[sess_idx],
            "title_id":      session_title[sess_idx],
            "device_id":     session_device[sess_idx],
            "event_time":    session_starts[sess_idx] + pd.to_timedelta(within_min, unit="m"),
            "event_type":    event_type,
            "position_sec":  position_sec.astype(int),
            "watch_seconds": watch_seconds.astype(int),
        })
        .sort_values(["customer_id", "event_time"])
        .reset_index(drop=True)
    )
    events.insert(0, "event_id", np.arange(1, len(events) + 1))
    return events


def generate(seed=SEED_DEFAULT):
    """Return a dict of four DataFrames: customers, titles, devices, events."""
    rng = np.random.default_rng(seed)
    customers = _make_customers(rng)
    titles    = _make_titles(rng)
    devices   = _make_devices(rng, customers)
    events    = _make_events(rng, customers, titles, devices)
    return {"customers": customers, "titles": titles, "devices": devices, "events": events}


if __name__ == "__main__":
    pd.set_option("display.width", 130)
    pd.set_option("display.max_columns", 20)
    d = generate()
    for name, df in d.items():
        print(f"\n=== {name} ({len(df)} rows) ===")
        print(df.head(8))
