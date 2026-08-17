# lambda/ — serverless edges of the pipeline

Two small Lambdas orchestrated by Airflow. Both are **stdlib + boto3 only**
(boto3 is in the Lambda runtime), so they ship as plain zips — no layers, no
`pip install`. Build with [`build.sh`](build.sh).

| Function | Source | Invoked by (DAG · task) | Job |
|----------|--------|-------------------------|-----|
| `streaming_imdb_mirror` | [imdb_mirror/handler.py](imdb_mirror/handler.py) | `imdb_monthly` · `lambda_mirror_imdb` | Stream IMDb `.tsv.gz` → `s3://…/imdb_base/<name>/` (multipart, no decompress) |
| `streaming_html_render` | [html_render/handler.py](html_render/handler.py) | `daily_rollup` · `lambda_render_html` | Query `streaming_reporting` via Athena → HTML dashboard in `s3://…/reports/` |

Function names match `airflow/dags/common/config.py` (`LAMBDA_IMDB_MIRROR`,
`LAMBDA_HTML_RENDER`).

## imdb_mirror

Streams `title.basics.tsv.gz` and `title.ratings.tsv.gz` from
`datasets.imdbws.com` to S3 with a multipart upload, one ~16 MiB part at a time —
never buffering a whole file in memory or `/tmp`, so file size is irrelevant to
the Lambda's memory/storage config. The bytes are mirrored **verbatim** (still
gzipped); `streaming_imdb_to_raw` reads `imdb_base/<name>/` as gzip CSV. A failure
aborts the multipart upload so no partial object lingers.

- Suggested config: 256 MB memory, 5 min timeout, network access (default VPC / no VPC is fine — it just needs egress to `datasets.imdbws.com`).
- IAM: `s3:CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `PutObject` on `acme-dw-streaming-xs2026/imdb_base/*`.

## html_render

Resolves the target day (`event["engagement_date"]`, else `MAX(engagement_date)`
per table), runs one Athena query per reporting mart, and writes
`reports/dashboard_<day>.html` **and** `reports/latest.html`.

- Suggested config: 256 MB memory, 2 min timeout.
- IAM: Athena (`StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`), Glue catalog read on `streaming_reporting`, S3 read on the table data + the Athena results prefix, and `PutObject` on `…/reports/*`.

### ⚠️ Athena results region
Athena runs in **us-east-1** but the data bucket is in **us-west-2**. Athena
requires its `OutputLocation` bucket to be in the **same region as Athena**. If
`s3://acme-dw-streaming-xs2026/` is us-west-2, point `ATHENA_OUTPUT` at a us-east-1
bucket (or run a us-east-1 results bucket created by Terraform). Tracked for the
`infra/terraform/` step.

## Environment variables

| Var | Default | Used by |
|-----|---------|---------|
| `STREAMING_S3_BUCKET` | `acme-dw-streaming-xs2026` | both |
| `IMDB_BASE_URL` | `https://datasets.imdbws.com` | imdb_mirror |
| `IMDB_S3_PREFIX` | `imdb_base` | imdb_mirror |
| `AWS_REGION` | `us-east-1` | html_render (Athena) |
| `STREAMING_REPORTING_DB` | `streaming_reporting` | html_render |
| `ATHENA_OUTPUT` | `s3://$BUCKET/athena-results/html_render/` | html_render |
| `ATHENA_WORKGROUP` | `primary` | html_render |
| `REPORTS_PREFIX` | `reports` | html_render |

## Build & deploy

```bash
./build.sh                       # -> dist/streaming_imdb_mirror.zip, dist/streaming_html_render.zip
aws lambda update-function-code --function-name streaming_imdb_mirror \
  --zip-file fileb://dist/streaming_imdb_mirror.zip --region us-east-1
aws lambda update-function-code --function-name streaming_html_render \
  --zip-file fileb://dist/streaming_html_render.zip --region us-east-1
```

The functions themselves (roles, env, memory/timeout) are created by
`infra/terraform/` — `build.sh` only ships code updates. Handler entrypoint:
`handler.handler`.
