output "webserver_url" {
  value       = "http://${aws_lb.airflow.dns_name}"
  description = "Airflow UI (allow a minute after apply for the service to go healthy)."
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.airflow.repository_url
  description = "Push the image here before applying the services."
}

output "cluster_name" {
  value = aws_ecs_cluster.airflow.name
}

output "db_endpoint" {
  value = aws_db_instance.airflow.address
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.airflow.cache_nodes[0].address
}
