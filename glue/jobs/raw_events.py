"""Glue job: streaming_raw_events — landing Iceberg -> raw Iceberg (hot path).

Ports skill #04's events path (run_skill_04_raw.py) to PySpark, interval-scoped.
v2 changes vs the Athena version:
  * reads the **landing Iceberg** table (not JSON.gz external tables);
  * processes the run's hour partition + a 90-min lookback to catch late events;
  * MERGE INTO raw (keyed by event_id) instead of full CTAS rebuild — idempotent;
  * routes invalid rows to a quarantine table instead of dropping them.

Transformations (DESIGN.md §3.4): schema cast, dedup on event_id, quarantine
(unknown event_type / future client clock / bad title_id / >24h stale), derive
event_date + event_hour, write partitioned by (event_date, event_hour).
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import common as c

VALID_EVENT_TYPES = ["play", "pause", "seek", "resume", "complete", "exit"]


def main():
    args = c.get_args(["data_interval_start", "data_interval_end"])
    start, end = c.parse_interval(args)
    lookback = start - timedelta(minutes=90)

    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_RAW)

    landing = c.t(c.DB_LANDING, "playback_events")
    raw = c.t(c.DB_RAW, "playback_events")
    quarantine = c.t(c.DB_RAW, "playback_events_quarantine")

    # Read the interval window (+lookback) from landing, pruning on the partition cols.
    df = (
        spark.read.format("iceberg").load(landing)
        .where(
            (F.col("server_received_at") >= F.lit(lookback))
            & (F.col("server_received_at") < F.lit(end))
        )
    )

    # Schema cast + derive partition/quality columns.
    df = (
        df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("server_received_at", F.to_timestamp("server_received_at"))
        .withColumn("position_ms", F.col("position_ms").cast("int"))
        .withColumn("bitrate_kbps", F.col("bitrate_kbps").cast("int"))
        .withColumn("title_id", F.trim("title_id"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("_loaded_at", F.current_timestamp())
    )

    # Quarantine predicate (DESIGN.md §3.4 / §6.1).
    lag_seconds = F.unix_timestamp("server_received_at") - F.unix_timestamp("event_timestamp")
    bad = (
        (~F.col("event_type").isin(VALID_EVENT_TYPES))
        | (F.col("event_timestamp") > F.expr("server_received_at + interval 5 minutes"))
        | (~F.col("title_id").rlike(r"^tt[0-9]{7,10}$"))
        | F.col("event_id").isNull()
        | (lag_seconds > 24 * 3600)
    )
    df = df.withColumn(
        "_quarantine_reason",
        F.when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("unknown_event_type"))
        .when(F.col("event_timestamp") > F.expr("server_received_at + interval 5 minutes"),
              F.lit("future_client_clock"))
        .when(~F.col("title_id").rlike(r"^tt[0-9]{7,10}$"), F.lit("invalid_title_id"))
        .when(F.col("event_id").isNull(), F.lit("missing_event_id"))
        .when(lag_seconds > 24 * 3600, F.lit("stale_late_arrival"))
        .otherwise(F.lit(None)),
    )

    quarantined = df.where(bad)
    clean = df.where(~bad)

    # Dedup on event_id, newest server_received_at wins.
    w = Window.partitionBy("event_id").orderBy(F.col("server_received_at").desc())
    clean = (
        clean.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_quarantine_reason")
    )

    clean.createOrReplaceTempView("raw_events_batch")

    # Create the target on first run, then MERGE for idempotent re-runs.
    if not c.table_exists(spark, raw):
        (clean.writeTo(raw)
            .using("iceberg")
            .partitionedBy("event_date", "event_hour")
            .tableProperty("format-version", "2")
            .tableProperty("write.target-file-size-bytes", str(128 * 1024 * 1024))
            .createOrReplace())
    else:
        spark.sql(f"""
            MERGE INTO {raw} target
            USING raw_events_batch source
            ON target.event_id = source.event_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if quarantined.take(1):
        if c.table_exists(spark, quarantine):
            quarantined.writeTo(quarantine).append()
        else:
            (quarantined.writeTo(quarantine).using("iceberg")
                .partitionedBy("event_date").createOrReplace())

    print(f"[raw_events] interval {start}..{end}: "
          f"clean={clean.count()} quarantined={quarantined.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
