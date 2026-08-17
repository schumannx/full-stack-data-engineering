# --- RDS Postgres: Airflow metadata DB + Celery result backend -----------------
resource "aws_db_subnet_group" "airflow" {
  name       = "${var.name}-db"
  subnet_ids = local.subnet_ids
}

resource "aws_db_instance" "airflow" {
  identifier        = "${var.name}-meta"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = "airflow"
  username = "airflow"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.airflow.name
  vpc_security_group_ids = [aws_security_group.data_stores.id]
  publicly_accessible    = false

  # On-demand demo stack: tear-down friendly.
  skip_final_snapshot = true
  deletion_protection = false
  apply_immediately   = true
}

# --- ElastiCache Redis: Celery broker -----------------------------------------
resource "aws_elasticache_subnet_group" "airflow" {
  name       = "${var.name}-redis"
  subnet_ids = local.subnet_ids
}

resource "aws_elasticache_cluster" "airflow" {
  cluster_id         = "${var.name}-broker"
  engine             = "redis"
  node_type          = var.redis_node_type
  num_cache_nodes    = 1
  port               = 6379
  subnet_group_name  = aws_elasticache_subnet_group.airflow.name
  security_group_ids = [aws_security_group.data_stores.id]
}
