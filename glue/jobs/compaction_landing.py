"""Glue job: streaming_compaction_landing — Iceberg maintenance on landing.

Defeats the streaming small-file problem (DESIGN.md §2.4, §5.2) by running, on the
landing playback_events table: rewrite_data_files (bin-pack to target size),
rewrite_manifests, and expire_snapshots. Runs on the `maintenance` Celery queue.
"""

from __future__ import annotations

import common as c

TARGET_BYTES = 128 * 1024 * 1024


def maintain(spark, table: str):
    spark.sql(f"""
        CALL {c.CATALOG}.system.rewrite_data_files(
            table => '{table}',
            options => map('target-file-size-bytes','{TARGET_BYTES}')
        )
    """)
    spark.sql(f"CALL {c.CATALOG}.system.rewrite_manifests(table => '{table}')")
    spark.sql(f"""
        CALL {c.CATALOG}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{_cutoff()}',
            retain_last => 5
        )
    """)
    print(f"[compaction] maintained {table}")


def _cutoff() -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")


def main():
    args = c.get_args()
    spark = c.build_spark(args["JOB_NAME"])
    maintain(spark, f"{c.DB_LANDING}.playback_events")
    spark.stop()


if __name__ == "__main__":
    main()
