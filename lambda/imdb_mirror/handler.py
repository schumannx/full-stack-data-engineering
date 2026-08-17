"""Lambda: streaming_imdb_mirror — stream IMDb's gzipped TSVs straight to S3.

Invoked by the ``imdb_monthly`` DAG (task ``lambda_mirror_imdb``) ahead of the
``streaming_imdb_to_raw`` Glue job. The Glue job reads ``imdb_base/<name>/`` as
gzip CSV, so we mirror the source ``.gz`` bytes **verbatim** — no decompression.

Why a Lambda and not a Glue/EC2 download: the files are big (title.basics is a
few hundred MB) but the work is pure I/O. We never hold a whole file in memory
or in ``/tmp``: bytes are pulled from the HTTP stream and pushed to S3 with a
**multipart upload**, one ~16 MB part at a time. That keeps us well under the
Lambda memory/ephemeral-storage limits regardless of file size.

See DESIGN.md §3.4 (skill #01) and PLAN_v2.md §2.
"""

from __future__ import annotations

import os
import urllib.request

import boto3

IMDB_BASE_URL = os.environ.get("IMDB_BASE_URL", "https://datasets.imdbws.com")
S3_BUCKET = os.environ.get("STREAMING_S3_BUCKET", "acme-dw-streaming-xs2026")
S3_PREFIX = os.environ.get("IMDB_S3_PREFIX", "imdb_base")

# Only the tables streaming_imdb_to_raw actually reads. Source basename -> the
# folder Glue globs (``imdb_base/title.basics/``).
FILES = ["title.basics.tsv.gz", "title.ratings.tsv.gz"]

# S3 multipart parts must be >= 5 MiB (except the final part). 16 MiB balances
# part count against per-part request overhead.
PART_SIZE = 16 * 1024 * 1024
HTTP_TIMEOUT = 60

s3 = boto3.client("s3")


def _dataset_name(filename: str) -> str:
    """``title.basics.tsv.gz`` -> ``title.basics`` (the folder Glue reads)."""
    return filename.split(".tsv")[0]


def _stream_one(filename: str) -> dict:
    src = f"{IMDB_BASE_URL}/{filename}"
    key = f"{S3_PREFIX}/{_dataset_name(filename)}/{filename}"

    mpu = s3.create_multipart_upload(Bucket=S3_BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    parts: list[dict] = []
    total = 0
    try:
        req = urllib.request.Request(src, headers={"User-Agent": "streaming-dw-imdb-mirror"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            buf = bytearray()
            while True:
                chunk = resp.read(1024 * 1024)
                if chunk:
                    buf.extend(chunk)
                    total += len(chunk)
                # Flush a part once we have a full PART_SIZE, or at EOF if any
                # bytes remain (the final part is allowed to be < 5 MiB).
                at_eof = not chunk
                while len(buf) >= PART_SIZE or (at_eof and buf):
                    body = bytes(buf[:PART_SIZE])
                    del buf[:PART_SIZE]
                    part_no = len(parts) + 1
                    resp_part = s3.upload_part(
                        Bucket=S3_BUCKET,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_no,
                        Body=body,
                    )
                    parts.append({"ETag": resp_part["ETag"], "PartNumber": part_no})
                    if at_eof and not buf:
                        break
                if at_eof:
                    break

        s3.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        # Don't leave a dangling upload accruing storage charges.
        s3.abort_multipart_upload(Bucket=S3_BUCKET, Key=key, UploadId=upload_id)
        raise

    return {"file": filename, "s3_key": key, "bytes": total, "parts": len(parts)}


def handler(event, context):
    results = [_stream_one(f) for f in FILES]
    print(f"[imdb_mirror] mirrored {len(results)} files: {results}")
    return {"status": "ok", "bucket": S3_BUCKET, "files": results}


if __name__ == "__main__":
    # Local smoke test (needs AWS creds + network).
    print(handler({}, None))
