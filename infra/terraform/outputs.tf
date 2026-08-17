output "data_bucket" {
  value       = var.data_bucket
  description = "Iceberg warehouse / landing / imdb_base / reports (us-west-2)."
}

output "ops_bucket" {
  value       = aws_s3_bucket.ops.id
  description = "us-east-1 bucket: Athena results + Glue scripts."
}

output "glue_databases" {
  value = { for k, db in aws_glue_catalog_database.zones : k => db.name }
}

output "glue_jobs" {
  value       = [for j in aws_glue_job.jobs : j.name]
  description = "Glue job names (must match airflow config.py GLUE{})."
}

output "athena_workgroup" {
  value = aws_athena_workgroup.streaming.name
}

output "airflow_execution_role_arn" {
  value       = aws_iam_role.airflow.arn
  description = "Pass to the airflow-ecs module (var.airflow_execution_role_arn) and use as aws_default in dev."
}

output "lambda_functions" {
  value = [aws_lambda_function.imdb_mirror.function_name, aws_lambda_function.html_render.function_name]
}

output "kafka_instance_id" {
  value       = var.enable_kafka ? aws_instance.kafka[0].id : null
  description = "SSM target for launching the producer (aws ssm start-session / send-command). null when enable_kafka=false."
}

output "kafka_private_dns" {
  value = var.enable_kafka ? aws_instance.kafka[0].private_dns : null
}

output "redshift_workgroup" {
  value = var.enable_redshift ? aws_redshiftserverless_workgroup.streaming[0].workgroup_name : null
}

output "redshift_namespace" {
  value = var.enable_redshift ? aws_redshiftserverless_namespace.streaming[0].namespace_name : null
}
