"""Glue job: streaming_processed_dims — build/refresh the conformed dimensions.

Ports skill #05's dimension build to PySpark with **SCD1 MERGE upserts** that
preserve surrogate keys across runs (the Athena version rebuilt keys each run;
that is unsafe once facts reference them). Builds: dim_title, dim_customer,
dim_device, dim_device_version, dim_date. The small static dims (dim_genre,
dim_time_of_day, dim_geography) are left as a follow-up.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import common as c


def upsert_scd1(spark, target: str, src_df, nat_key: str, surrogate: str):
    """SCD1 upsert that assigns stable surrogate keys to new natural keys.

    Existing rows keep their surrogate key; brand-new natural keys get
    max(existing)+row_number. Idempotent: re-running with the same source is a no-op
    beyond column updates.
    """
    src_df.createOrReplaceTempView("src")
    if not c.table_exists(spark, target):
        keyed = src_df.withColumn(
            surrogate, F.row_number().over(Window.orderBy(nat_key)).cast("bigint")
        )
        keyed.writeTo(target).using("iceberg").createOrReplace()
        return

    existing = spark.read.format("iceberg").load(target)
    max_key = existing.agg(F.coalesce(F.max(surrogate), F.lit(0)).alias("m")).collect()[0]["m"]
    existing_keys = existing.select(nat_key)
    new_rows = src_df.join(existing_keys, nat_key, "left_anti")
    new_keyed = new_rows.withColumn(
        surrogate,
        (F.lit(max_key) + F.row_number().over(Window.orderBy(nat_key))).cast("bigint"),
    )
    new_keyed.createOrReplaceTempView("new_keyed")
    # Update attributes for existing natural keys.
    update_cols = [col for col in src_df.columns if col != nat_key]
    set_clause = ", ".join(f"target.{col} = src.{col}" for col in update_cols)
    spark.sql(f"""
        MERGE INTO {target} target USING src
        ON target.{nat_key} = src.{nat_key}
        WHEN MATCHED THEN UPDATE SET {set_clause}
    """)
    new_keyed.writeTo(target).append()


def main():
    args = c.get_args()
    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_PROCESSED)

    # dim_title from raw IMDb title_basics (produced by imdb_to_raw).
    titles = spark.read.format("iceberg").load(c.t(c.DB_RAW, "title_basics")).select(
        "tconst", "primary_title", "title_type", "genres",
        "runtime_minutes", "imdb_rating", "num_votes",
    ).withColumn("_updated_at", F.current_timestamp())
    upsert_scd1(spark, c.t(c.DB_PROCESSED, "dim_title"), titles, "tconst", "title_key")

    # dim_customer (SCD1 from raw_customer_profiles).
    cust = spark.read.format("iceberg").load(c.t(c.DB_RAW, "customer_profiles")).select(
        "customer_id", "email_hash", "signup_date", "country",
        "plan_tier", "age_band", "household_size",
    ).withColumn("_updated_at", F.current_timestamp())
    upsert_scd1(spark, c.t(c.DB_PROCESSED, "dim_customer"), cust, "customer_id", "customer_key")

    # dim_device (distinct device attributes).
    dev = (
        spark.read.format("iceberg").load(c.t(c.DB_RAW, "device_registry"))
        .select("device_id", "device_type", "platform", "device_model").distinct()
        .withColumn("_updated_at", F.current_timestamp())
    )
    upsert_scd1(spark, c.t(c.DB_PROCESSED, "dim_device"), dev, "device_id", "device_key")

    # dim_device_version (child of dim_device; needs device_key).
    dim_device = spark.read.format("iceberg").load(c.t(c.DB_PROCESSED, "dim_device"))
    dv = (
        spark.read.format("iceberg").load(c.t(c.DB_RAW, "device_registry"))
        .join(dim_device.select("device_id", "device_key"), "device_id", "left")
        .select("device_version_id", "device_id", "device_key",
                "os_version", "app_version", "is_deprecated")
        .withColumn("_updated_at", F.current_timestamp())
    )
    upsert_scd1(spark, c.t(c.DB_PROCESSED, "dim_device_version"),
                dv, "device_version_id", "device_version_key")

    # dim_date (static; full rebuild is cheap and key is deterministic YYYYMMDD).
    dates = (
        spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) AS full_date")
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_premiere_friday", F.dayofweek("full_date") == 6)
        .withColumn("season",
            F.when(F.month("full_date").isin(12, 1, 2), "Winter")
            .when(F.month("full_date").isin(3, 4, 5), "Spring")
            .when(F.month("full_date").isin(6, 7, 8), "Summer")
            .otherwise("Fall"))
    )
    dates.writeTo(c.t(c.DB_PROCESSED, "dim_date")).using("iceberg").createOrReplace()

    print("[processed_dims] dims refreshed")
    spark.stop()


if __name__ == "__main__":
    main()
