resource "aws_ecs_cluster" "airflow" {
  name = var.name
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "airflow" {
  name              = "/ecs/${var.name}"
  retention_in_days = 14
}

# One Fargate task definition + service per Airflow role (webserver, scheduler,
# triggerer, worker). All share the same image, env, and secrets.
resource "aws_ecs_task_definition" "svc" {
  for_each = local.services

  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = var.airflow_execution_role_arn

  container_definitions = jsonencode([
    {
      name        = each.key
      image       = var.airflow_image
      command     = each.value.command
      essential   = true
      environment = local.airflow_env
      secrets     = local.airflow_secrets
      portMappings = each.key == "webserver" ? [
        { containerPort = 8080, protocol = "tcp" }
      ] : []
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = each.key
        }
      }
    }
  ])
}

resource "aws_ecs_service" "svc" {
  for_each = local.services

  name            = each.key
  cluster         = aws_ecs_cluster.airflow.id
  task_definition = aws_ecs_task_definition.svc[each.key].arn
  desired_count   = each.value.desired
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.subnet_ids
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = true # default subnets are public; needed to pull image/secrets
  }

  # Only the webserver sits behind the ALB.
  dynamic "load_balancer" {
    for_each = each.key == "webserver" ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.webserver.arn
      container_name   = "webserver"
      container_port   = 8080
    }
  }

  depends_on = [aws_lb_listener.http]
}
