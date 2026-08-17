# =============================================================================
# Per-zone Glue job roles
# Each Glue job runs as the role for the zone it writes to: read the whole
# warehouse, write only its own zone prefixes (PLAN_v2.md §6).
# =============================================================================
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_zone" {
  for_each           = local.glue_role_write_prefixes
  name               = "${local.name}-glue-${replace(each.key, "_", "-")}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# AWS-managed baseline (CloudWatch logs, ENI mgmt, etc.) for Glue.
resource "aws_iam_role_policy_attachment" "glue_service" {
  for_each   = aws_iam_role.glue_zone
  role       = each.value.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_zone_inline" {
  for_each = local.glue_role_write_prefixes
  name     = "zone-access"
  role     = aws_iam_role.glue_zone[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadWarehouse"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*"]
      },
      {
        Sid      = "WriteZonePrefixes"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject"]
        Resource = [for p in each.value : "${local.data_bucket_arn}/${p}"]
      },
      {
        Sid      = "OpsBucketScriptsAndTemp"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      },
      {
        Sid      = "GlueCatalog"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:BatchCreatePartition", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:UpdatePartition", "glue:CreatePartition", "glue:DeletePartition"]
        Resource = local.glue_catalog_arns
      },
    ]
  })
}

# =============================================================================
# Airflow execution role
# Used by the prod ECS tasks (airflow-ecs module references this ARN) and, in
# dev, mirrors the permissions the mounted ~/.aws creds need (PLAN_v2.md §6).
# =============================================================================
data "aws_iam_policy_document" "airflow_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
  # Allow the account's IAM users (dev) to assume it for parity testing.
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${local.partition}:iam::${local.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${local.name}-airflow-execution"
  assume_role_policy = data.aws_iam_policy_document.airflow_assume.json
}

resource "aws_iam_role_policy" "airflow_inline" {
  name = "orchestration"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Glue"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun", "glue:GetJob"]
        Resource = "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:job/streaming_*"
      },
      {
        Sid      = "Lambda"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = "arn:${local.partition}:lambda:${var.aws_region}:${local.account_id}:function:streaming_*"
      },
      {
        Sid      = "RedshiftData"
        Effect   = "Allow"
        Action   = ["redshift-data:*", "redshift-serverless:GetCredentials", "redshift:GetClusterCredentialsWithIAM"]
        Resource = "*"
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup"]
        Resource = "*"
      },
      {
        Sid      = "GlueCatalogRead"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = local.glue_catalog_arns
      },
      {
        Sid      = "S3"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*", aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      },
    ]
  })
}

# =============================================================================
# Lambda roles
# =============================================================================
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# --- imdb_mirror: multipart-write to imdb_base/ + logs ---
resource "aws_iam_role" "lambda_imdb_mirror" {
  name               = "${local.name}-lambda-imdb-mirror"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "imdb_mirror_logs" {
  role       = aws_iam_role.lambda_imdb_mirror.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "imdb_mirror_s3" {
  name = "imdb-base-write"
  role = aws_iam_role.lambda_imdb_mirror.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts"]
      Resource = ["${local.data_bucket_arn}/imdb_base/*"]
    }]
  })
}

# --- html_render: Athena + catalog read + reports write + athena-results rw ---
resource "aws_iam_role" "lambda_html_render" {
  name               = "${local.name}-lambda-html-render"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "html_render_logs" {
  role       = aws_iam_role.lambda_html_render.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "html_render" {
  name = "athena-and-reports"
  role = aws_iam_role.lambda_html_render.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"]
        Resource = "*"
      },
      {
        Sid      = "GlueCatalogRead"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = local.glue_catalog_arns
      },
      {
        Sid      = "ReadReportingData"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*"]
      },
      {
        Sid      = "WriteReports"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${local.data_bucket_arn}/reports/*"]
      },
      {
        Sid = "AthenaResults"
        Effect = "Allow"
        # GetBucketLocation is required: Athena verifies the output bucket's region
        # before StartQueryExecution, else "Unable to verify/create output bucket".
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      },
    ]
  })
}

# =============================================================================
# Kafka EC2 instance profile (broker + consumer)
# SSM Session Manager + producer/consumer S3/Glue access (PLAN_v2.md §6).
# =============================================================================
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "kafka" {
  count              = var.enable_kafka ? 1 : 0
  name               = "${local.name}-kafka-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "kafka_ssm" {
  count      = var.enable_kafka ? 1 : 0
  role       = aws_iam_role.kafka[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "kafka_inline" {
  count = var.enable_kafka ? 1 : 0
  name  = "landing-iceberg-write"
  role  = aws_iam_role.kafka[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WarehouseRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*"]
      },
      {
        Sid      = "OpsBucketScripts"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/kafka/*"]
      },
      {
        Sid      = "LandingWrite"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.data_bucket_arn}/${local.glue_databases.landing}.db/*"]
      },
      {
        Sid      = "GlueCatalogLanding"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:CreatePartition", "glue:UpdatePartition"]
        Resource = local.glue_catalog_arns
      },
    ]
  })
}

resource "aws_iam_instance_profile" "kafka" {
  count = var.enable_kafka ? 1 : 0
  name  = "${local.name}-kafka"
  role  = aws_iam_role.kafka[0].name
}
