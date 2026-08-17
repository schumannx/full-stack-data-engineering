"""Glue job: streaming_reporting_aggregates — processed -> reporting pre-aggregates.

Ports skill #06 (run_skill_06_reporting.py) to PySpark. Builds the four reporting
Iceberg tables Redshift Spectrum / Athena query:
  content_engagement_daily, device_engagement_daily, genre_mix_daily,
  title_completion_funnel.
Day-scoped via ``--engagement_date``; overwrites that day's partition.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

import common as c


def main():
    args = c.get_args(["engagement_date"])
    day = args.get("engagement_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_REPORTING)

    s = (
        spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "fact_view_sessions"))
        .where(F.col("session_start_date") == F.lit(day))
    )
    dim_t = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_title"))
    dim_d = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_device"))

    completed = F.sum(F.when(F.col("was_completed"), 1).otherwise(0))
    n = F.count(F.lit(1))

    # content_engagement_daily
    content = (
        s.join(dim_t, "title_key", "left")
        .groupBy(F.col("session_start_date").alias("engagement_date"),
                 "title_key", "tconst", "primary_title", "title_type", "genres")
        .agg(
            F.countDistinct("customer_key").alias("distinct_viewers"),
            n.alias("sessions_count"),
            F.sum("watch_duration_seconds").cast("bigint").alias("total_watch_seconds"),
            completed.alias("completion_count"),
            (completed / n).cast("decimal(4,3)").alias("completion_rate"),
            F.avg("watch_duration_seconds").cast("int").alias("avg_session_seconds"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    _write_day(spark, content, c.t(c.DB_REPORTING, "content_engagement_daily"), "engagement_date")

    # device_engagement_daily
    device = (
        s.join(dim_d, "device_key", "left")
        .groupBy(F.col("session_start_date").alias("engagement_date"), "device_type", "platform")
        .agg(
            n.alias("sessions_count"),
            F.countDistinct("customer_key").alias("distinct_viewers"),
            F.sum("watch_duration_seconds").cast("bigint").alias("total_watch_seconds"),
            F.avg("watch_duration_seconds").cast("int").alias("avg_session_seconds"),
            (completed / n).cast("decimal(4,3)").alias("completion_rate"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    _write_day(spark, device, c.t(c.DB_REPORTING, "device_engagement_daily"), "engagement_date")

    # genre_mix_daily (explode pipe/comma genres, normalize to % of day)
    gs = (
        s.join(dim_t, "title_key", "left")
        .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
        .withColumn("genre", F.trim("genre"))
        .select(F.col("session_start_date").alias("engagement_date"), "genre", "watch_duration_seconds")
    )
    totals = gs.groupBy("engagement_date").agg(F.sum("watch_duration_seconds").alias("day_total"))
    genre = (
        gs.groupBy("engagement_date", "genre")
        .agg(F.sum("watch_duration_seconds").cast("bigint").alias("watch_seconds"))
        .join(totals, "engagement_date")
        .withColumn("pct_of_day", (F.col("watch_seconds") * 100.0 / F.col("day_total")).cast("decimal(5,2)"))
        .drop("day_total")
        .withColumn("_built_at", F.current_timestamp())
    )
    _write_day(spark, genre, c.t(c.DB_REPORTING, "genre_mix_daily"), "engagement_date")

    # title_completion_funnel (10 buckets of completion_pct)
    funnel = (
        s.join(dim_t, "title_key", "left")
        .withColumn("engagement_date", F.col("session_start_date"))
        .withColumn("bucket", F.least((F.col("completion_pct") * 10).cast("int"), F.lit(9)))
        .groupBy("engagement_date", "title_key", "tconst", "primary_title", "title_type", "bucket")
        .agg(F.count(F.lit(1)).alias("sessions_in_bucket"))
        .withColumn("_built_at", F.current_timestamp())
    )
    _write_day(spark, funnel, c.t(c.DB_REPORTING, "title_completion_funnel"), "engagement_date")

    print(f"[reporting_aggregates] day={day} done")
    spark.stop()


def _write_day(spark, df, target: str, part: str):
    if not c.table_exists(spark, target):
        df.writeTo(target).using("iceberg").partitionedBy(part).createOrReplace()
    else:
        df.writeTo(target).overwritePartitions()


if __name__ == "__main__":
    main()
