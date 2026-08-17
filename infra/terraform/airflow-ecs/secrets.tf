# Generated credentials. NOTE: these land in Terraform state — fine for an
# on-demand demo stack; use a real secrets workflow for anything persistent.
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "random_password" "webserver_secret" {
  length  = 32
  special = false
}

# Fernet key must be 32 url-safe-base64 bytes; random_id.b64_std fits.
resource "random_id" "fernet" {
  byte_length = 32
}

locals {
  db_host    = aws_db_instance.airflow.address
  db_name    = aws_db_instance.airflow.db_name
  db_user    = aws_db_instance.airflow.username
  db_pass    = random_password.db.result
  redis_host = aws_elasticache_cluster.airflow.cache_nodes[0].address

  sql_alchemy_conn = "postgresql+psycopg2://${local.db_user}:${local.db_pass}@${local.db_host}:5432/${local.db_name}"
  result_backend   = "db+postgresql://${local.db_user}:${local.db_pass}@${local.db_host}:5432/${local.db_name}"
  broker_url       = "redis://${local.redis_host}:6379/0"
}

# Sensitive Airflow settings injected into containers via ECS `secrets`.
# for_each is driven by a static key set (NOT the sensitive value map, which
# would mark the whole for_each argument sensitive and be rejected).
locals {
  secret_keys = toset(["sql_alchemy_conn", "result_backend", "fernet_key", "webserver_secret"])
  secret_values = {
    sql_alchemy_conn = local.sql_alchemy_conn
    result_backend   = local.result_backend
    fernet_key       = random_id.fernet.b64_std
    webserver_secret = random_password.webserver_secret.result
  }
}

resource "aws_secretsmanager_secret" "this" {
  for_each                = local.secret_keys
  name                    = "${var.name}/${each.key}"
  recovery_window_in_days = 0 # immediate delete on destroy
}

resource "aws_secretsmanager_secret_version" "this" {
  for_each      = local.secret_keys
  secret_id     = aws_secretsmanager_secret.this[each.key].id
  secret_string = local.secret_values[each.key]
}
