"""
Upload generator output to S3.

Walks the local generator_out/ directory (which already uses
yyyy=*/mm=*/dd=*/hh=*/ partition layout) and uploads each .jsonl.gz file
to s3://<bucket>/<prefix>/yyyy=*/mm=*/dd=*/hh=*/<filename>.

Requires AWS credentials configured (via `aws configure` or env vars).
Checks creds before attempting any upload.
"""

import argparse
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Upload generator output to S3")
    p.add_argument("--bucket", required=True, help="S3 bucket name (e.g. acme-dw-streaming-xs2026)")
    p.add_argument("--prefix", default="generator_replay/",
                   help="S3 key prefix (default: generator_replay/)")
    p.add_argument("--source-dir", default="./generator_out",
                   help="Local directory to upload (default: ./generator_out)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be uploaded without doing it")
    args = p.parse_args()

    src = Path(args.source_dir)
    if not src.exists():
        sys.exit(f"FAIL: source directory does not exist: {src}")

    files = sorted(src.glob("**/*.jsonl.gz"))
    if not files:
        sys.exit(f"FAIL: no .jsonl.gz files found under {src}")

    print(f"Found {len(files)} file(s) to upload from {src}/")

    # Build S3 key for each file (preserve partition path under source-dir)
    uploads = []
    for f in files:
        rel = f.relative_to(src)
        key = args.prefix.rstrip("/") + "/" + str(rel).replace("\\", "/")
        uploads.append((f, key))

    if args.dry_run:
        print("DRY RUN - would upload:")
        for f, key in uploads:
            print(f"  {f}  ->  s3://{args.bucket}/{key}")
        return

    # Import boto3 only when actually uploading (so dry-run works without it)
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError
    except ImportError:
        sys.exit("FAIL: boto3 not installed. Run: pip install -r requirements.txt")

    # Check credentials are configured
    try:
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        print(f"  AWS account: {identity['Account']}, ARN: {identity['Arn']}")
    except NoCredentialsError:
        sys.exit("FAIL: AWS credentials not configured. Run: aws configure")
    except ClientError as e:
        sys.exit(f"FAIL: AWS STS call failed: {e}")

    # Verify bucket exists and is writable (HEAD on the bucket)
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=args.bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "404":
            sys.exit(f"FAIL: bucket '{args.bucket}' does not exist. Create it with:\n"
                     f"  aws s3 mb s3://{args.bucket} --region us-west-2")
        elif code == "403":
            sys.exit(f"FAIL: no permission to access bucket '{args.bucket}'")
        else:
            sys.exit(f"FAIL: bucket check failed: {e}")

    print(f"  bucket s3://{args.bucket} accessible")
    print()

    # Upload
    uploaded_count = 0
    total_bytes = 0
    for f, key in uploads:
        size = f.stat().st_size
        print(f"  uploading {f.name} ({size:,} bytes) -> s3://{args.bucket}/{key}")
        try:
            s3.upload_file(str(f), args.bucket, key)
            uploaded_count += 1
            total_bytes += size
        except ClientError as e:
            print(f"    FAIL: {e}")
            sys.exit(1)

    print()
    print(f"Done: uploaded {uploaded_count} file(s), {total_bytes:,} bytes total")
    print(f"Verify with: aws s3 ls s3://{args.bucket}/{args.prefix} --recursive")


if __name__ == "__main__":
    main()
