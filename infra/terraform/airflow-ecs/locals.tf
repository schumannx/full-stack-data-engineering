locals {
  # Non-sensitive Airflow settings shared by every container.
  airflow_env = [
    { name = "AIRFLOW__CORE__EXECUTOR", value = "CeleryExecutor" },
    { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "False" },
    { name = "AIRFLOW__CELERY__BROKER_URL", value = local.broker_url },
    { name = "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", value = "True" },
    { name = "AIRFLOW__WEBSERVER__EXPOSE_CONFIG", value = "False" },
    # DAGs reach AWS through the task role (no static keys); region for boto3.
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
  ]

  # Sensitive settings pulled from Secrets Manager at container start.
  airflow_secrets = [
    { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", valueFrom = aws_secretsmanager_secret.this["sql_alchemy_conn"].arn },
    { name = "AIRFLOW__CELERY__RESULT_BACKEND", valueFrom = aws_secretsmanager_secret.this["result_backend"].arn },
    { name = "AIRFLOW__CORE__FERNET_KEY", valueFrom = aws_secretsmanager_secret.this["fernet_key"].arn },
    { name = "AIRFLOW__WEBSERVER__SECRET_KEY", valueFrom = aws_secretsmanager_secret.this["webserver_secret"].arn },
  ]

  # role -> [command]. The official apache/airflow entrypoint dispatches these.
  services = {
    webserver = { command = ["webserver"], cpu = 512, memory = 1024, desired = 1 }
    scheduler = { command = ["scheduler"], cpu = 512, memory = 1024, desired = 1 }
    triggerer = { command = ["triggerer"], cpu = 256, memory = 512, desired = 1 }
    worker    = { command = ["celery", "worker"], cpu = 1024, memory = 2048, desired = var.worker_desired_count }
  }
}
