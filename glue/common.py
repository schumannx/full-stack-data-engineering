"""Shared helpers for the Streaming DW Glue PySpark jobs.

Every job:
  * builds a SparkSession wired to the Glue Data Catalog as an Iceberg catalog
    (catalog name ``glue_catalog``), so tables are addressed as
    ``glue_catalog.<db>.<table>``;
  * parses Glue job args (``--JOB_NAME`` plus job-specific args like
    ``--data_interval_start``) via getResolvedOptions;
  * is idempotent — keyed on the interval / day it is given, so retries and
    backfills produce identical output (DESIGN.md §4.5).

Run on Glue 4.0 with ``--datalake-formats=iceberg``. Locally you can run the
same scripts under spark-submit with the Iceberg + AWS bundles on the classpath.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.sql import SparkSession

# --- Database names (one per zone) ---------------------------------------------
DB_LANDING = "streaming_landing"
DB_RAW = "streaming_raw"
DB_PROCESSED = "streaming_processed"
DB_REPORTING = "streaming_reporting"

CATALOG = "glue_catalog"
# Data bucket is us-west-2; Glue catalog/warehouse region is us-east-1 (see config).
WAREHOUSE = "s3://acme-dw-streaming-xs2026/"
# Region of the warehouse bucket — needed so Iceberg's S3FileIO signs/addresses
# writes for us-west-2 instead of the job's us-east-1 (else S3 returns 301).
DATA_REGION = "us-west-2"


def build_spark(app_name: str) -> SparkSession:
    """SparkSession with the Glue catalog registered as an Iceberg catalog."""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        # Cross-region: catalog runs us-east-1, warehouse bucket is us-west-2.
        # cross-region-access-enabled lets S3FileIO auto-detect the bucket's region
        # and route/sign there (Iceberg >=1.4 / Glue 5.0); without it S3 returns 301.
        # s3.region is set too as a belt-and-suspenders hint.
        .config(f"spark.sql.catalog.{CATALOG}.s3.cross-region-access-enabled", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", DATA_REGION)
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


def get_args(extra: list[str] | None = None) -> dict:
    """Resolve Glue job args. JOB_NAME is always present; `extra` lists job args."""
    names = ["JOB_NAME"] + (extra or [])
    return getResolvedOptions(sys.argv, names)


def parse_interval(args: dict) -> tuple[datetime, datetime]:
    """(data_interval_start, data_interval_end) as tz-aware UTC datetimes.

    Defaults to the previous full hour if not supplied, so the script is runnable
    ad-hoc for smoke tests.
    """
    end_raw = args.get("data_interval_end")
    start_raw = args.get("data_interval_start")
    if start_raw:
        start = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    else:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = now - timedelta(hours=1)
    if end_raw:
        end = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
    else:
        end = start + timedelta(hours=1)
    return start, end


def t(db: str, table: str) -> str:
    """Fully-qualified Iceberg table identifier."""
    return f"{CATALOG}.{db}.{table}"


def ensure_namespace(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{db}")


def table_exists(spark: SparkSession, fqtn: str) -> bool:
    """Whether an Iceberg table exists in the Glue catalog.

    Pass the fully-qualified ``glue_catalog.<db>.<table>`` identifier (i.e. the
    output of :func:`t`). The bare ``db.table`` form resolves against Spark's
    default session catalog — where these Iceberg tables do NOT live — so it would
    spuriously report False; always qualify with the catalog name.
    """
    return spark.catalog.tableExists(fqtn)
