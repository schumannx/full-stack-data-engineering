terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Portfolio project: local state. Swap for an S3+DynamoDB backend before any
  # shared/team use.
  # backend "s3" {}
}

# Glue Data Catalog, Glue jobs, Athena, Lambda, Redshift, Kafka EC2 all live in
# us-east-1 (the org SCP blocks us-west-2 for our IAM user's compute APIs).
provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.common_tags
  }
}

# The data bucket / Iceberg warehouse pre-dates this stack and lives in
# us-west-2. Resources that must be co-located with the data use this alias.
provider "aws" {
  alias  = "data"
  region = var.data_region
  default_tags {
    tags = local.common_tags
  }
}
