"""Glue job: streaming_fact_daily_engagement — periodic-snapshot daily fact.

Ports CREATE_FACT_DAILY_ENGAGEMENT from skill #05. One row per
(customer, title, day). Idempotent: overwrites the target day's partition
(DESIGN.md §3.3 — "idempotent re-runs replace the day's partition"). Day comes
from the ``--engagement_date`` arg (the DAG passes ``{{ ds }}``).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

import common as c


def main():
    args = c.get_args(["engagement_date"])
    day = args.get("engagement_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_PROCESSED)

    sessions = (
        spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "fact_view_sessions"))
        .where(F.col("session_start_date") == F.lit(day))
    )

    daily = (
        sessions.groupBy("session_start_date", "customer_key", "title_key").agg(
            F.countDistinct("session_id").alias("sessions_count"),
            F.sum("watch_duration_seconds").cast("int").alias("total_watch_seconds"),
            F.avg("completion_pct").cast("decimal(4,3)").alias("completion_pct"),
            F.max("session_end_ts").alias("last_session_end_ts"),
        )
        .withColumnRenamed("session_start_date", "engagement_date")
        .withColumn("date_key", F.date_format("engagement_date", "yyyyMMdd").cast("int"))
        .withColumn("_built_at", F.current_timestamp())
        .select("engagement_date", "date_key", "customer_key", "title_key",
                "sessions_count", "total_watch_seconds", "completion_pct",
                "last_session_end_ts", "_built_at")
    )

    target = c.t(c.DB_PROCESSED, "fact_daily_engagement")
    if not c.table_exists(spark, target):
        daily.writeTo(target).using("iceberg").partitionedBy("engagement_date").createOrReplace()
    else:
        # Idempotent day replace: overwrite just this partition.
        daily.writeTo(target).overwritePartitions()

    print(f"[fact_daily_engagement] day={day}: rows={daily.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
