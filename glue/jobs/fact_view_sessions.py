"""Glue job: streaming_fact_view_sessions — accumulating-snapshot session fact.

Ports CREATE_FACT_VIEW_SESSIONS from skill #05. A session row is UPDATED as new
events arrive across micro-batches (DESIGN.md §3.3), so this recomputes every
session **touched in the interval** from the full set of that session's events,
then MERGEs into the fact keyed by session_id. 24h late-update window.
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import functions as F

import common as c


def main():
    args = c.get_args(["data_interval_start", "data_interval_end"])
    start, end = c.parse_interval(args)
    window_start = start - timedelta(hours=24)  # late-update window

    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_PROCESSED)

    events = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "fact_playback_events"))
    # Sessions that received an event in this interval.
    touched = (
        events.where((F.col("event_timestamp") >= F.lit(start)) & (F.col("event_timestamp") < F.lit(end)))
        .select("session_id").distinct()
    )
    # Recompute those sessions from all their events within the late window.
    scoped = (
        events.where(F.col("event_timestamp") >= F.lit(window_start))
        .join(touched, "session_id", "left_semi")
    )

    aggs = (
        scoped.groupBy("session_id").agg(
            F.first("customer_key", ignorenulls=True).alias("customer_key"),
            F.first("title_key", ignorenulls=True).alias("title_key"),
            F.first("device_key", ignorenulls=True).alias("device_key"),
            F.first("device_version_key", ignorenulls=True).alias("device_version_key"),
            F.min("event_timestamp").alias("session_start_ts"),
            F.max("event_timestamp").alias("session_end_ts"),
            F.max("position_ms").alias("max_position_ms"),
            F.sum(F.when(F.col("event_type") == "pause", 1).otherwise(0)).alias("pause_count"),
            F.sum(F.when(F.col("event_type") == "seek", 1).otherwise(0)).alias("seek_count"),
            (F.max(F.when(F.col("event_type") == "complete", 1).otherwise(0)) == 1).alias("was_completed"),
        )
        .withColumn("session_start_date", F.to_date("session_start_ts"))
    )

    dim_t = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_title")).select("title_key", "runtime_minutes")
    sessions = (
        aggs.join(dim_t, "title_key", "left")
        .withColumn("date_key", F.date_format("session_start_date", "yyyyMMdd").cast("int"))
        .withColumn("watch_duration_seconds",
                    (F.unix_timestamp("session_end_ts") - F.unix_timestamp("session_start_ts")).cast("int"))
        .withColumn("content_duration_seconds", (F.col("runtime_minutes") * 60).cast("int"))
        .withColumn("completion_pct",
                    F.least(F.col("max_position_ms") / (F.col("runtime_minutes") * 60.0 * 1000), F.lit(1.0)).cast("decimal(4,3)"))
        .withColumn("was_force_closed", F.lit(False))
        .withColumn("_updated_at", F.current_timestamp())
        .select("session_id", "customer_key", "title_key", "device_key", "device_version_key",
                "date_key", "session_start_ts", "session_end_ts", "watch_duration_seconds",
                "content_duration_seconds", "completion_pct", "pause_count", "seek_count",
                "was_completed", "was_force_closed", "_updated_at", "session_start_date")
    )

    sessions.createOrReplaceTempView("sessions_batch")
    target = c.t(c.DB_PROCESSED, "fact_view_sessions")
    if not c.table_exists(spark, target):
        sessions.writeTo(target).using("iceberg").partitionedBy("session_start_date").createOrReplace()
    else:
        spark.sql(f"""
            MERGE INTO {target} target USING sessions_batch source
            ON target.session_id = source.session_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    print(f"[fact_view_sessions] interval {start}..{end}: sessions={sessions.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
