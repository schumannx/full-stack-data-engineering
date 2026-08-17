"""Glue job: streaming_fact_playback_events — raw events -> transaction-grain fact.

Ports CREATE_FACT_PLAYBACK_EVENTS from skill #05. Interval-scoped: reads only the
raw rows in the run window, resolves dimension FKs, and MERGEs into the fact
keyed by event_id (idempotent; DESIGN.md §4.5). Partitioned by event_date.
"""

from __future__ import annotations

from pyspark.sql import functions as F

import common as c


def main():
    args = c.get_args(["data_interval_start", "data_interval_end"])
    start, end = c.parse_interval(args)

    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_PROCESSED)

    e = (
        spark.read.format("iceberg").load(c.t(c.DB_RAW, "playback_events"))
        .where((F.col("event_timestamp") >= F.lit(start)) & (F.col("event_timestamp") < F.lit(end)))
    )
    dim_c = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_customer")).select("customer_id", "customer_key")
    dim_t = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_title")).select("tconst", "title_key")
    dim_d = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_device")).select("device_id", "device_key")
    dim_dv = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_device_version")).select("device_version_id", "device_version_key")

    fact = (
        e.join(dim_c, "customer_id", "left")
        .join(dim_t, e.title_id == dim_t.tconst, "left")
        .join(dim_d, "device_id", "left")
        .join(dim_dv, "device_version_id", "left")
        .select(
            "event_id", "session_id", "customer_key", "title_key", "device_key",
            "device_version_key",
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            "event_type", "event_timestamp", "position_ms", "bitrate_kbps",
            "geo_country", "event_date", "event_hour",
        )
    )

    fact.createOrReplaceTempView("fact_events_batch")
    target = c.t(c.DB_PROCESSED, "fact_playback_events")
    if not c.table_exists(spark, target):
        fact.writeTo(target).using("iceberg").partitionedBy("event_date").createOrReplace()
    else:
        spark.sql(f"""
            MERGE INTO {target} target USING fact_events_batch source
            ON target.event_id = source.event_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    print(f"[fact_playback_events] interval {start}..{end}: rows={fact.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
