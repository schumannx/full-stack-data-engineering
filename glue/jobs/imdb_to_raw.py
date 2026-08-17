"""Glue job: streaming_imdb_to_raw — IMDb TSV.gz master -> raw Iceberg.

Reads the gzipped IMDb TSVs mirrored to imdb_base/ by the imdb_mirror Lambda
(DESIGN.md §3.4 skill #01), converts the ``\\N`` null sentinels to true NULLs,
and writes a conformed ``streaming_raw.title_basics`` Iceberg table that
processed_dims builds dim_title from. Full monthly overwrite.
"""

from __future__ import annotations

from pyspark.sql import functions as F

import common as c

IMDB_BASE = "s3://acme-dw-streaming-xs2026/imdb_base"


def _read_tsv(spark, name: str):
    df = (
        spark.read.option("sep", "\t").option("header", True)
        .option("nullValue", "\\N").csv(f"{IMDB_BASE}/{name}/")
    )
    # Belt-and-suspenders: any remaining \N -> NULL across string cols.
    for col in df.columns:
        df = df.withColumn(col, F.when(F.col(col) == "\\N", None).otherwise(F.col(col)))
    return df


def main():
    args = c.get_args()
    spark = c.build_spark(args["JOB_NAME"])
    c.ensure_namespace(spark, c.DB_RAW)

    basics = _read_tsv(spark, "title.basics")
    ratings = _read_tsv(spark, "title.ratings")

    title_basics = (
        basics.join(ratings, "tconst", "left")
        .select(
            "tconst",
            F.col("primaryTitle").alias("primary_title"),
            F.col("titleType").alias("title_type"),
            "genres",
            F.col("runtimeMinutes").cast("int").alias("runtime_minutes"),
            F.col("averageRating").cast("decimal(3,1)").alias("imdb_rating"),
            F.col("numVotes").cast("int").alias("num_votes"),
        )
        # Keep only watchable types to bound dim_title size.
        .where(F.col("title_type").isin("movie", "tvSeries", "tvMovie", "tvMiniSeries"))
    )

    title_basics.writeTo(c.t(c.DB_RAW, "title_basics")).using("iceberg").createOrReplace()
    print(f"[imdb_to_raw] title_basics rows={title_basics.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
