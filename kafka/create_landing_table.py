#!/usr/bin/env python3
"""One-time: create the landing Iceberg table in the Glue catalog.

The consumer (consumer.py) appends to this table; it must exist first. Partitioned
by (event_date, event_hour) to match raw/processed layout and enable hour-level
pruning (DESIGN.md §2.4, §3.3). Idempotent — skips if the table already exists.

  AWS_REGION=us-east-1 STREAMING_S3_BUCKET=acme-dw-streaming-xs2026 \
      python create_landing_table.py
"""

from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

import config

# Field IDs must be stable and unique. event_date/event_hour are the partition cols.
SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "session_id", StringType(), required=False),
    NestedField(3, "customer_id", StringType(), required=False),
    NestedField(4, "title_id", StringType(), required=False),
    NestedField(5, "device_id", StringType(), required=False),
    NestedField(6, "device_version_id", StringType(), required=False),
    NestedField(7, "event_type", StringType(), required=False),
    NestedField(8, "event_timestamp", TimestamptzType(), required=False),
    NestedField(9, "server_received_at", TimestamptzType(), required=False),
    NestedField(10, "position_ms", IntegerType(), required=False),
    NestedField(11, "bitrate_kbps", IntegerType(), required=False),
    NestedField(12, "geo_country", StringType(), required=False),
    NestedField(13, "schema_version", IntegerType(), required=False),
    NestedField(14, "event_date", StringType(), required=False),
    NestedField(15, "event_hour", IntegerType(), required=False),
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=14, field_id=1000, transform=IdentityTransform(), name="event_date"),
    PartitionField(source_id=15, field_id=1001, transform=IdentityTransform(), name="event_hour"),
)


def get_catalog():
    # pyiceberg uses prefixed property names (plain `region_name` is ignored, which
    # surfaces as botocore NoRegionError). The Glue catalog is us-east-1 but the
    # warehouse bucket is us-west-2, so the S3 FileIO needs its own region or writes
    # 301-redirect (same cross-region gotcha the Glue Spark jobs hit).
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": config.WAREHOUSE,
            "glue.region": config.AWS_REGION,
            "s3.region": config.DATA_REGION,
        },
    )


def main():
    catalog = get_catalog()
    try:
        catalog.create_namespace(config.GLUE_DATABASE)
        print(f"Created namespace {config.GLUE_DATABASE}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace {config.GLUE_DATABASE} already exists")

    try:
        catalog.create_table(
            identifier=config.TABLE_IDENTIFIER,
            schema=SCHEMA,
            partition_spec=PARTITION_SPEC,
            properties={
                "write.target-file-size-bytes": str(128 * 1024 * 1024),
                "format-version": "2",
            },
        )
        print(f"Created table {config.TABLE_IDENTIFIER}")
    except TableAlreadyExistsError:
        print(f"Table {config.TABLE_IDENTIFIER} already exists — nothing to do")


if __name__ == "__main__":
    main()
