variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type    = string
  default = "streaming-airflow"
}

variable "airflow_image" {
  type        = string
  description = "Full image URI pushed to the ECR repo (built from ../../../airflow/Dockerfile). e.g. <acct>.dkr.ecr.us-east-1.amazonaws.com/streaming-airflow:latest"
}

variable "airflow_execution_role_arn" {
  type        = string
  description = "The orchestration role from the root module (output airflow_execution_role_arn). Used as the ECS task role so DAGs can call Glue/Lambda/Athena/Redshift."
}

variable "vpc_id" {
  type        = string
  default     = ""
  description = "Empty = default VPC."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Empty = default VPC subnets. Fargate services + RDS + Redis use these."
}

variable "alb_ingress_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "CIDR allowed to reach the Airflow webserver via the ALB. Lock this to your IP for a demo."
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "worker_desired_count" {
  type    = number
  default = 1
}

variable "airflow_admin_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Password for the initial Airflow admin user (set via TF_VAR_airflow_admin_password). Used by the one-off db-init task documented in the README."
}
