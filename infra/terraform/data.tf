data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  # Glue Data Catalog ARNs for this account/region (catalog + all dbs + tables).
  glue_catalog_arns = [
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog",
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/*",
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/*/*",
  ]
}
