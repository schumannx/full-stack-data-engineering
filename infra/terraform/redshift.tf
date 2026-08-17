# Redshift Serverless for the serving layer. Spectrum reads the reporting Iceberg
# tables through a Glue external schema (created via SQL post-apply — see README).

# --- Spectrum / external-schema IAM role --------------------------------------
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_spectrum" {
  count              = var.enable_redshift ? 1 : 0
  name               = "${local.name}-redshift-spectrum"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

resource "aws_iam_role_policy" "redshift_spectrum" {
  count = var.enable_redshift ? 1 : 0
  name  = "spectrum-glue-s3"
  role  = aws_iam_role.redshift_spectrum[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GlueCatalogRead"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"]
        Resource = local.glue_catalog_arns
      },
      {
        Sid      = "ReadWarehouse"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.data_bucket_arn, "${local.data_bucket_arn}/*"]
      },
    ]
  })
}

# --- Namespace (DB + credentials + attached roles) ----------------------------
resource "aws_redshiftserverless_namespace" "streaming" {
  count          = var.enable_redshift ? 1 : 0
  namespace_name = "${local.name}-ns"
  db_name        = "dev"
  admin_username = var.redshift_admin_username

  # Use a provided password, else let Redshift manage one in Secrets Manager.
  admin_user_password   = var.redshift_admin_password != "" ? var.redshift_admin_password : null
  manage_admin_password = var.redshift_admin_password == "" ? true : null

  iam_roles            = [aws_iam_role.redshift_spectrum[0].arn]
  default_iam_role_arn = aws_iam_role.redshift_spectrum[0].arn
}

# --- Workgroup (compute endpoint) ---------------------------------------------
resource "aws_redshiftserverless_workgroup" "streaming" {
  count          = var.enable_redshift ? 1 : 0
  namespace_name = aws_redshiftserverless_namespace.streaming[0].namespace_name
  workgroup_name = "${local.name}-wg"
  base_capacity  = var.redshift_base_capacity

  subnet_ids          = tolist(local.subnet_ids)
  security_group_ids  = [aws_security_group.redshift[0].id]
  publicly_accessible = false
}
