terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Separate state from the root module — this stack is applied on demand and
  # destroyed after a demo.
  # backend "s3" {}
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project   = "streaming-dw"
      Component = "airflow-ecs"
      ManagedBy = "terraform"
    }
  }
}
