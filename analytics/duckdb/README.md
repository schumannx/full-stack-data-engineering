# DuckDB validation layer

A cheap, fast way to **validate the marts/reports** by reading the *same* Iceberg
tables Athena serves — straight from S3, in-process, **free** (no Athena scan cost).
It's an ad-hoc mirror of `common/validation.py` and the dbt tests.

## What `validate.sql` checks
1. `content_engagement_daily.sessions_count` == a fresh recompute from `fact_view_sessions` (0 mismatches).
2. `completion_rate` within [0, 1].
3. `genre_mix_daily.pct_of_day` sums to ~100 per day.
4. No duplicate grain in the content mart.
5. Summary: top titles by watch time.

## Run it locally (zero infra)
```bash
brew install duckdb            # one-time
duckdb < analytics/duckdb/validate.sql
```
Creds come from your `~/.aws` (the `credential_chain` secret). Proven against the
real warehouse — all four checks pass.

## Run it on the EC2 node
A read-only DuckDB box (gated, apply-on-demand):
```bash
cd infra/terraform
tofu apply -var='enable_duckdb_ec2=true'
tofu output duckdb_ec2_instance          # => i-0...
aws ssm start-session --target i-0... --region us-east-1
#   on the box:
duckdb < /opt/streaming/analytics/duckdb/validate.sql
# done:
aws ec2 stop-instances --instance-ids i-0... --region us-east-1   # ~$0 stopped
```

## Key technical notes
- **Cross-region:** the warehouse is us-west-2; the `CREATE SECRET ... REGION 'us-west-2'`
  handles S3. The Glue `ATTACH` path is **not** used — the Glue catalog is us-east-1 but
  the data is us-west-2, and a single secret region can't serve both (DuckDB issue #265).
  So tables are read by their **S3 root** instead.
- **Snapshot resolution:** `SET unsafe_enable_version_guessing = true` lets `iceberg_scan`
  find the latest metadata from the table root (Glue-written tables have no version-hint).
- **Read-only by design:** the EC2 instance role grants only S3 + Glue *reads*, so a
  validation query can never mutate the pipeline.
