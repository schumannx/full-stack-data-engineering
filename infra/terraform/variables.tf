variable "project" {
  type        = string
  default     = "streaming-dw"
  description = "Tag/name prefix for all resources."
}

variable "env" {
  type        = string
  default     = "dev"
  description = "Environment name (dev/prod) — only the data plane lives here; Airflow prod is the separate airflow-ecs module."
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Region for Glue catalog, Glue jobs, Athena, Lambda, Redshift, Kafka EC2."
}

variable "data_region" {
  type        = string
  default     = "us-west-2"
  description = "Region of the pre-existing data bucket / Iceberg warehouse."
}

# --- S3 ------------------------------------------------------------------------
variable "data_bucket" {
  type        = string
  default     = "acme-dw-streaming-xs2026"
  description = "Iceberg warehouse + landing + imdb_base + reports. Pre-exists; terraform import it (see README) or set manage_data_bucket=false."
}

variable "manage_data_bucket" {
  type        = bool
  default     = true
  description = "If false, the data bucket is referenced (data source) but not managed by this stack."
}

variable "ops_bucket" {
  type        = string
  default     = "acme-dw-streaming-xs2026-use1-ops"
  description = "us-east-1 bucket for Athena query results + Glue job scripts (must be same region as Athena)."
}

# --- Glue ----------------------------------------------------------------------
variable "glue_version" {
  type        = string
  default     = "5.0"
  description = "AWS Glue version for all PySpark jobs (5.0 = Spark 3.5 / Iceberg 1.7, needed for the s3.region cross-region override)."
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 4
}

# --- Feature flags (cost control) ----------------------------------------------
variable "enable_kafka" {
  type        = bool
  default     = true
  description = "Create the always-on Kafka broker/consumer EC2 instance (~the only 24/7 cost). Set false for a Glue/Lambda-only deploy."
}

variable "enable_redshift" {
  type        = bool
  default     = true
  description = "Create the Redshift Serverless namespace + workgroup. Set false until you need the serving layer."
}

# --- Kafka EC2 -----------------------------------------------------------------
variable "kafka_instance_type" {
  type    = string
  default = "t3.small"
}

variable "consumer_instance_type" {
  type    = string
  default = "t3.small"
}

variable "key_pair_name" {
  type        = string
  default     = ""
  description = "Optional EC2 key pair for SSH. Leave empty to rely solely on SSM Session Manager."
}

variable "ssh_ingress_cidr" {
  type        = string
  default     = ""
  description = "Optional CIDR allowed to SSH (22) to the Kafka broker. Empty = no SSH ingress (SSM only)."
}

# --- Airflow on EC2 (blue/green) ----------------------------------------------
variable "enable_airflow_ec2" {
  type        = bool
  default     = false
  description = "Stand up blue + green Airflow EC2 instances (apply-on-demand for the blue/green demo). Off by default — these run the full Airflow stack and cost while up."
}

variable "airflow_instance_type" {
  type        = string
  default     = "t3.medium"
  description = "Airflow runs the whole CeleryExecutor stack in Docker; 4GB (t3.medium) is the practical minimum."
}

variable "airflow_repo_url" {
  type        = string
  default     = "https://github.com/schumannx/full-stack-data-engineering.git"
  description = "Public repo the instance clones to build/run the Airflow compose."
}

variable "airflow_repo_branch" {
  type        = string
  default     = "main"
  description = "Branch/ref to deploy. Must contain the 3.2 + Cosmos code (after merging the PRs, that's main)."
}

# --- DuckDB validation node ----------------------------------------------------
variable "enable_duckdb_ec2" {
  type        = bool
  default     = false
  description = "Stand up a small READ-ONLY DuckDB node to validate marts/reports off the shared S3 warehouse (apply-on-demand). Off by default."
}

variable "duckdb_instance_type" {
  type        = string
  default     = "t3.small"
  description = "DuckDB is lightweight; the marts are tiny, so t3.small is plenty."
}

# --- Alerting ------------------------------------------------------------------
variable "alert_email" {
  type        = string
  default     = ""
  description = "Email subscribed to the SNS alert topic (task failures + daily DQ scorecard). Empty = topic/sub not created. Confirm the AWS email link after apply."
}

# --- Redshift Serverless -------------------------------------------------------
variable "redshift_base_capacity" {
  type        = number
  default     = 8
  description = "Redshift Serverless base RPUs (min 8)."
}

variable "redshift_admin_username" {
  type    = string
  default = "dwadmin"
}

variable "redshift_admin_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Admin password for the Redshift namespace. Set via TF_VAR_redshift_admin_password; empty lets Redshift manage it in Secrets Manager."
}

# --- Networking ----------------------------------------------------------------
variable "vpc_id" {
  type        = string
  default     = ""
  description = "VPC for Kafka EC2 + Redshift. Empty = use the account's default VPC."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Subnets for Redshift (>=2 AZs) and Kafka. Empty = default VPC subnets."
}
