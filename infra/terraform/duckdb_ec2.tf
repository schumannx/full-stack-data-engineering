# =============================================================================
# DuckDB validation node on EC2 (apply-on-demand; gated by enable_duckdb_ec2)
#
# A small READ-ONLY box: installs the DuckDB CLI and points iceberg_scan at the
# shared S3 warehouse to validate marts/reports for free (no Athena cost). It can
# ONLY read (S3 + Glue catalog), so a stray query can never mutate the pipeline.
# See analytics/duckdb/validate.sql. Reuses var.airflow_repo_url / _branch.
# =============================================================================

data "aws_ssm_parameter" "al2023_duckdb" {
  count = var.enable_duckdb_ec2 ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_security_group" "duckdb" {
  count       = var.enable_duckdb_ec2 ? 1 : 0
  name        = "${local.name}-duckdb"
  description = "DuckDB validation node (SSM by default; optional SSH)"
  vpc_id      = local.vpc_id

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
    description = "All outbound (SSM, S3, Glue, duckdb download)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# READ-ONLY instance role: S3 read on the warehouse + ops, Glue catalog read. No writes.
resource "aws_iam_role" "duckdb" {
  count              = var.enable_duckdb_ec2 ? 1 : 0
  name               = "${local.name}-duckdb-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "duckdb_ssm" {
  count      = var.enable_duckdb_ec2 ? 1 : 0
  role       = aws_iam_role.duckdb[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "duckdb_inline" {
  count = var.enable_duckdb_ec2 ? 1 : 0
  name  = "read-only-warehouse"
  role  = aws_iam_role.duckdb[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadWarehouseAndOps"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*", aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      },
      {
        Sid      = "GlueCatalogRead"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
        Resource = local.glue_catalog_arns
      },
    ]
  })
}

resource "aws_iam_instance_profile" "duckdb" {
  count = var.enable_duckdb_ec2 ? 1 : 0
  name  = "${local.name}-duckdb"
  role  = aws_iam_role.duckdb[0].name
}

resource "aws_instance" "duckdb" {
  count                  = var.enable_duckdb_ec2 ? 1 : 0
  ami                    = data.aws_ssm_parameter.al2023_duckdb[0].value
  instance_type          = var.duckdb_instance_type
  subnet_id              = tolist(local.subnet_ids)[0]
  vpc_security_group_ids = [aws_security_group.duckdb[0].id]
  iam_instance_profile   = aws_iam_instance_profile.duckdb[0].name
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null
  associate_public_ip_address = var.ssh_ingress_cidr != "" ? true : null

  user_data = templatefile("${path.module}/templates/duckdb_user_data.sh.tftpl", {
    repo_url = var.airflow_repo_url
    branch   = var.airflow_repo_branch
  })
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = { Name = "${local.name}-duckdb" }
}

output "duckdb_ec2_instance" {
  description = "DuckDB validation node instance ID (SSM in and run analytics/duckdb/validate.sql)."
  value       = var.enable_duckdb_ec2 ? aws_instance.duckdb[0].id : null
}
