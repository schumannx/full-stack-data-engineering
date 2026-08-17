"""Glue job: streaming_raw_snapshots — daily customer/device snapshots -> raw.

Ports the customer_profiles + device_registry paths from skill #04. These remain
daily JSONL.gz full snapshots in landing (DESIGN.md §3.4); each run fully
overwrites the corresponding raw Iceberg table (snapshot semantics, no dedup).
"""

from __future__ import annotations

from pyspark.sql import functions as F

import common as c

LANDING_BASE = "s3://acme-dw-streaming-xs2026/landing/streaming"


def main():
    args = c.get_args()
    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_RAW)

    # customer_profiles
    cust = spark.read.json(f"{LANDING_BASE}/customer_profiles/")
    cust = (
        cust.select(
            "customer_id", "email_hash",
            F.to_date("signup_date").alias("signup_date"),
            "country", "plan_tier", "age_band",
            F.col("household_size").cast("int").alias("household_size"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
        )
    )
    cust.writeTo(c.t(c.DB_RAW, "customer_profiles")).using("iceberg").createOrReplace()

    # device_registry
    dev = spark.read.json(f"{LANDING_BASE}/device_registry/")
    dev = dev.select(
        "device_id", "device_version_id", "device_type", "platform",
        "device_model", "os_version", "app_version",
        F.col("is_deprecated").cast("boolean").alias("is_deprecated"),
    )
    dev.writeTo(c.t(c.DB_RAW, "device_registry")).using("iceberg").createOrReplace()

    print(f"[raw_snapshots] customers={cust.count()} devices={dev.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
