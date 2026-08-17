"""Glue job: streaming_compaction_facts — Iceberg maintenance on the fact tables.

Same maintenance routine as compaction_landing, applied to the high-churn
processed facts that the 15-min micro-batches append/MERGE into. Runs on the
`maintenance` Celery queue, daily off-peak.
"""

from __future__ import annotations

import common as c
from compaction_landing import maintain

FACT_TABLES = [
    f"{c.DB_PROCESSED}.fact_playback_events",
    f"{c.DB_PROCESSED}.fact_view_sessions",
]


def main():
    args = c.get_args()
    spark = c.build_spark(args["JOB_NAME"])
    for tbl in FACT_TABLES:
        # `tbl` is the bare db.table form the Iceberg CALL needs; qualify with the
        # catalog only for the existence probe.
        if c.table_exists(spark, f"{c.CATALOG}.{tbl}"):
            maintain(spark, tbl)
    spark.stop()


if __name__ == "__main__":
    main()
