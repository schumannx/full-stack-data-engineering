"""
One-time setup: create Glue databases for each zone in us-east-1.

Idempotent — safe to re-run.
"""

import boto3
from botocore.exceptions import ClientError

import config

_glue = boto3.client("glue", region_name=config.GLUE_REGION)


DATABASES = [
    (config.DB_LANDING, "Landing zone — external tables on raw JSON.gz drops"),
    (config.DB_RAW, "Raw zone — Parquet, deduped, schema-cast"),
    (config.DB_PROCESSED, "Processed zone — Iceberg dim and fact tables (Kimball model)"),
    (config.DB_REPORTING, "Reporting zone — Iceberg pre-aggregates"),
]


def create_database(name: str, description: str) -> None:
    try:
        _glue.create_database(
            DatabaseInput={"Name": name, "Description": description}
        )
        print(f"  created: {name}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "AlreadyExistsException":
            print(f"  exists:  {name}")
        else:
            raise


def main():
    print(f"Glue region: {config.GLUE_REGION}")
    print(f"Athena region: {config.ATHENA_REGION}")
    print(f"Data bucket: {config.DATA_BUCKET} (us-west-2)")
    print(f"Results bucket: {config.RESULTS_BUCKET} (us-east-1)")
    print()
    print("Creating Glue databases:")
    for name, desc in DATABASES:
        create_database(name, desc)
    print()
    print("Setup complete. Verify with: aws glue get-databases --region us-east-1")


if __name__ == "__main__":
    main()
