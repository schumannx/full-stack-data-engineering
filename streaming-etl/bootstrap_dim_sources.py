"""
Bootstrap customer_profiles and device_registry snapshots to S3 landing zone.

Reads the customer + device universes the generator produced (same seed = same universe)
and writes them as JSONL.gz files to:
  s3://acme-dw-streaming-xs2026/landing/streaming/customer_profiles/yyyy=*/mm=*/dd=*/
  s3://acme-dw-streaming-xs2026/landing/streaming/device_registry/yyyy=*/mm=*/dd=*/

These match what skill #03 says daily snapshots should look like.
"""

import gzip
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np

# Reuse the generator's universe builders
sys.path.insert(0, str(Path(__file__).parent.parent / "streaming-generator"))
from generator import build_customer_universe, build_device_universe  # noqa: E402

import config

PARTITION_DATE = datetime(2026, 5, 2, tzinfo=timezone.utc)
SEED = 42
N_CUSTOMERS = 100


def customer_to_jsonl(customer, signup_date_base: datetime) -> dict:
    """Convert generator Customer dataclass to landing JSONL spec (skill #03)."""
    # Synthesize email_hash + signup_date from customer_id (deterministic)
    h = hashlib.sha256(customer.customer_id.encode()).hexdigest()
    plan_tier = ["basic", "standard", "premium"][int(h[:2], 16) % 3]
    age_band = ["18-24", "25-34", "35-49", "50+"][int(h[2:4], 16) % 4]
    household_size = (int(h[4:6], 16) % 5) + 1
    return {
        "customer_id": customer.customer_id,
        "email_hash": h,
        "signup_date": signup_date_base.date().isoformat(),
        "country": customer.country,
        "plan_tier": plan_tier,
        "age_band": age_band,
        "household_size": household_size,
        "created_at": signup_date_base.isoformat().replace("+00:00", "Z"),
        "updated_at": signup_date_base.isoformat().replace("+00:00", "Z"),
    }


def device_to_jsonl(device) -> dict:
    """Convert DeviceVersion dataclass to landing JSONL spec."""
    return {
        "device_id": device.device_id,
        "device_version_id": device.device_version_id,
        "device_type": device.device_type,
        "platform": device.platform,
        "device_model": device.model,
        "os_version": device.os_version,
        "app_version": device.app_version,
        "is_deprecated": False,
    }


def write_jsonl_gz(records: list, local_path: Path) -> int:
    """Write records to a local .jsonl.gz file."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(local_path, "wt") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return local_path.stat().st_size


def upload(local_path: Path, s3_key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), config.DATA_BUCKET, s3_key)
    print(f"  uploaded -> s3://{config.DATA_BUCKET}/{s3_key}")


def main():
    rng = np.random.default_rng(SEED)
    customers = build_customer_universe(N_CUSTOMERS, rng)
    devices = build_device_universe()
    print(f"Built universe: {len(customers)} customers, {len(devices)} device versions")

    partition = (
        f"yyyy={PARTITION_DATE.year}/"
        f"mm={PARTITION_DATE.month:02d}/"
        f"dd={PARTITION_DATE.day:02d}"
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        # customer_profiles
        cust_records = [customer_to_jsonl(c, PARTITION_DATE) for c in customers]
        cust_path = tmp / "customers_full.jsonl.gz"
        size = write_jsonl_gz(cust_records, cust_path)
        cust_key = f"landing/streaming/customer_profiles/{partition}/customers_full.jsonl.gz"
        print(f"  customer_profiles: {len(cust_records)} rows, {size} bytes")
        upload(cust_path, cust_key)

        # device_registry
        dev_records = [device_to_jsonl(d) for d in devices]
        dev_path = tmp / "devices_full.jsonl.gz"
        size = write_jsonl_gz(dev_records, dev_path)
        dev_key = f"landing/streaming/device_registry/{partition}/devices_full.jsonl.gz"
        print(f"  device_registry: {len(dev_records)} rows, {size} bytes")
        upload(dev_path, dev_key)

    print("\nDone — dim source snapshots in landing zone.")


if __name__ == "__main__":
    main()
