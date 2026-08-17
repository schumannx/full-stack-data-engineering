# Use the account's default VPC/subnets unless explicit ones are provided.
data "aws_vpc" "default" {
  count   = var.vpc_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }
}

locals {
  vpc_id     = var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id
  subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : tolist(data.aws_subnets.default[0].ids)
}

# --- Kafka broker security group ----------------------------------------------
resource "aws_security_group" "kafka" {
  count       = var.enable_kafka ? 1 : 0
  name        = "${local.name}-kafka"
  description = "Kafka broker (KRaft) + consumer"
  vpc_id      = local.vpc_id

  # Kafka client port, intra-SG only (producer/consumer reach the broker).
  ingress {
    description = "Kafka client (intra-SG)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    self        = true
  }

  # Optional SSH; SSM Session Manager is the default access path.
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
    description = "All outbound (SSM, S3, Glue, package installs)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Redshift Serverless security group ---------------------------------------
resource "aws_security_group" "redshift" {
  count       = var.enable_redshift ? 1 : 0
  name        = "${local.name}-redshift"
  description = "Redshift Serverless workgroup"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
