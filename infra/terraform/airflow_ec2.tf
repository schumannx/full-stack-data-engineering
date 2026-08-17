# =============================================================================
# Airflow on EC2 — blue/green (apply-on-demand; gated by enable_airflow_ec2)
#
# Two instances (blue, green) that each clone the repo and run the Airflow 3.2
# CeleryExecutor compose against the SHARED data plane (Glue/S3/Athena) via this
# instance role. Only ONE is "active" (DAGs unpaused) at a time — cutover with
# deploy/blue-green/cutover.sh over SSM. See deploy/blue-green/README.md.
# =============================================================================

locals {
  # color -> host UI port (only matters if you ever co-locate both on one box;
  # on separate instances each binds its own :port). Drives for_each.
  airflow_stacks = var.enable_airflow_ec2 ? {
    blue  = 8080
    green = 8081
  } : {}
}

# --- Latest Amazon Linux 2023 AMI ---------------------------------------------
data "aws_ssm_parameter" "al2023_airflow" {
  count = var.enable_airflow_ec2 ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# --- Security group ------------------------------------------------------------
resource "aws_security_group" "airflow_ec2" {
  count       = var.enable_airflow_ec2 ? 1 : 0
  name        = "${local.name}-airflow"
  description = "Airflow blue/green EC2 (SSM by default; optional SSH + UI ingress)"
  vpc_id      = local.vpc_id

  # Optional: reach the Airflow UI (8080 blue / 8081 green) from your IP.
  dynamic "ingress" {
    for_each = var.ssh_ingress_cidr != "" ? [var.ssh_ingress_cidr] : []
    content {
      description = "Airflow UI"
      from_port   = 8080
      to_port     = 8081
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  dynamic "ingress" {
    for_each = var.ssh_ingress_cidr != "" ? [var.ssh_ingress_cidr] : []
    content {
      description = "SSH"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    description = "All outbound (SSM, S3, Glue, Athena, image pulls)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Instance role: orchestration + dbt-athena writes --------------------------
# Broader than the ECS airflow role because dbt runs IN the worker (Cosmos LOCAL),
# so this role — not a Glue job role — does the Iceberg writes for the marts.
resource "aws_iam_role" "airflow_ec2" {
  count              = var.enable_airflow_ec2 ? 1 : 0
  name               = "${local.name}-airflow-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "airflow_ec2_ssm" {
  count      = var.enable_airflow_ec2 ? 1 : 0
  role       = aws_iam_role.airflow_ec2[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "airflow_ec2_inline" {
  count = var.enable_airflow_ec2 ? 1 : 0
  name  = "orchestration-and-dbt"
  role  = aws_iam_role.airflow_ec2[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GlueJobs"
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
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup"]
        Resource = "*"
      },
      {
        # Full catalog rw: dbt-athena creates/merges the reporting Iceberg tables.
        Sid      = "GlueCatalogReadWrite"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:DeletePartition"]
        Resource = local.glue_catalog_arns
      },
      {
        # Read whole warehouse; write+delete needed for dbt Iceberg MERGE/expire.
        Sid      = "WarehouseS3"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:AbortMultipartUpload"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*"]
      },
      {
        Sid      = "OpsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      },
      {
        Sid      = "PublishAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "arn:${local.partition}:sns:${var.aws_region}:${local.account_id}:${local.name}-*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "airflow_ec2" {
  count = var.enable_airflow_ec2 ? 1 : 0
  name  = "${local.name}-airflow-ec2"
  role  = aws_iam_role.airflow_ec2[0].name
}

# --- Blue + green instances ----------------------------------------------------
resource "aws_instance" "airflow" {
  for_each = local.airflow_stacks

  ami                    = data.aws_ssm_parameter.al2023_airflow[0].value
  instance_type          = var.airflow_instance_type
  subnet_id              = tolist(local.subnet_ids)[0]
  vpc_security_group_ids = [aws_security_group.airflow_ec2[0].id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_ec2[0].name
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null
  # Public IP only if you opened ingress; otherwise SSM-only (private).
  associate_public_ip_address = var.ssh_ingress_cidr != "" ? true : null

  # IMDSv2 with hop limit 2 so the Docker containers (one extra network hop) can
  # reach the instance role for AWS creds.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = templatefile("${path.module}/templates/airflow_user_data.sh.tftpl", {
    color       = each.key
    http_port   = each.value
    project     = "streaming_${each.key}"
    region      = var.aws_region
    data_bucket = var.data_bucket
    repo_url    = var.airflow_repo_url
    branch      = var.airflow_repo_branch
  })
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 40 # image build + dbt venv + logs
    volume_type = "gp3"
  }

  tags = { Name = "${local.name}-airflow-${each.key}" }
}

output "airflow_ec2_instances" {
  description = "Blue/green Airflow EC2 instance IDs (use with SSM for cutover/logs)."
  value       = { for k, i in aws_instance.airflow : k => i.id }
}

output "airflow_ec2_public_ips" {
  description = "Public IPs (populated only when ssh_ingress_cidr opens the UI)."
  value       = { for k, i in aws_instance.airflow : k => i.public_ip }
}
