# --- Data bucket (us-west-2): Iceberg warehouse, landing, imdb_base, reports ---
# Pre-exists from the streaming-etl baseline. Manage it here only if
# manage_data_bucket=true (after a `terraform import`); otherwise reference it.

resource "aws_s3_bucket" "data" {
  count    = var.manage_data_bucket ? 1 : 0
  provider = aws.data
  bucket   = var.data_bucket
}

data "aws_s3_bucket" "data" {
  count    = var.manage_data_bucket ? 0 : 1
  provider = aws.data
  bucket   = var.data_bucket
}

locals {
  data_bucket_arn = var.manage_data_bucket ? aws_s3_bucket.data[0].arn : data.aws_s3_bucket.data[0].arn
}

resource "aws_s3_bucket_versioning" "data" {
  count    = var.manage_data_bucket ? 1 : 0
  provider = aws.data
  bucket   = aws_s3_bucket.data[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  count    = var.manage_data_bucket ? 1 : 0
  provider = aws.data
  bucket   = aws_s3_bucket.data[0].id

  # Iceberg compaction leaves orphaned data/metadata behind expire_snapshots;
  # reclaim noncurrent versions and failed multipart uploads.
  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  count                   = var.manage_data_bucket ? 1 : 0
  provider                = aws.data
  bucket                  = aws_s3_bucket.data[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Ops bucket (us-east-1): Athena results + Glue scripts ---------------------
# Athena requires its result location in the same region as the workgroup, and
# the data bucket is us-west-2 — hence a dedicated us-east-1 bucket.

resource "aws_s3_bucket" "ops" {
  bucket = var.ops_bucket
}

resource "aws_s3_bucket_public_access_block" "ops" {
  bucket                  = aws_s3_bucket.ops.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "ops" {
  bucket = aws_s3_bucket.ops.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 14
    }
  }

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# --- Upload Glue job scripts + shared modules to the ops bucket ----------------
resource "aws_s3_object" "glue_common" {
  bucket = aws_s3_bucket.ops.id
  key    = "glue-scripts/common.py"
  source = "${local.glue_dir}/common.py"
  etag   = filemd5("${local.glue_dir}/common.py")
}

resource "aws_s3_object" "glue_job_scripts" {
  for_each = fileset(local.glue_dir, "jobs/*.py")
  bucket   = aws_s3_bucket.ops.id
  key      = "glue-scripts/${each.value}"
  source   = "${local.glue_dir}/${each.value}"
  etag     = filemd5("${local.glue_dir}/${each.value}")
}
