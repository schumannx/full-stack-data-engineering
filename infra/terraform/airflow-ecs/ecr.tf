# ECR repo for the custom Airflow image (built from ../../../airflow/Dockerfile,
# which adds apache-airflow-providers-amazon). Push before applying the services.
resource "aws_ecr_repository" "airflow" {
  name         = var.name
  force_delete = true # demo stack: allow destroy even with images present

  image_scanning_configuration {
    scan_on_push = true
  }
}
