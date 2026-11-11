# One router task. The connection registry lives in the process, so a second
# task would split the org's daemons between two registries. Deploys therefore
# stop the old task before starting the new one (a short reconnect gap, which
# daemons ride out), and the circuit breaker rolls back to the previous task
# definition if the new one never turns healthy.

resource "aws_cloudwatch_log_group" "router" {
  #checkov:skip=CKV_AWS_158:Log data is encrypted by CloudWatch by default; a customer-managed key adds cost this recipe does not assume.
  #checkov:skip=CKV_AWS_338:Retention is a variable (14 days by default) on purpose; keeping router logs a year is the operator's call.
  name              = "/ecs/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "router" {
  #checkov:skip=CKV_AWS_65:Container Insights bills custom metrics per task; the ALB and service metrics cover one task.
  name = var.name
}

locals {
  # The router's own settings. They win over extra_environment, so a stray
  # key there cannot point history at a different table or drop the TLS
  # declaration.
  router_environment = merge(var.extra_environment, {
    SEALMESH_GITHUB_ORG        = var.github_org
    SEALMESH_PROJECTS          = jsonencode(var.projects)
    SEALMESH_LISTEN            = "0.0.0.0:${local.router_port}"
    SEALMESH_TLS_TERMINATED_BY = "alb"
    SEALMESH_HISTORY_BACKEND   = "dynamodb"
    DYNAMODB_ROOM_HISTORY      = aws_dynamodb_table.room_history.name
  })
}

resource "aws_ecs_task_definition" "router" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name                   = "router"
      image                  = local.image
      essential              = true
      readonlyRootFilesystem = true
      privileged             = false

      portMappings = [
        { containerPort = local.router_port, protocol = "tcp" }
      ]

      environment = [
        for key in sort(keys(local.router_environment)) : { name = key, value = local.router_environment[key] }
      ]

      # Values ECS fetches from Parameter Store at start; never plain env.
      secrets = [
        for key in sort(keys(var.secret_parameter_arns)) : { name = key, valueFrom = var.secret_parameter_arns[key] }
      ]

      healthCheck = {
        command     = ["CMD", "/usr/local/bin/sealmesh-server", "healthcheck"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 10
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.router.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "router"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "router" {
  name                               = var.name
  cluster                            = aws_ecs_cluster.router.id
  task_definition                    = aws_ecs_task_definition.router.arn
  launch_type                        = "FARGATE"
  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  health_check_grace_period_seconds  = 30
  enable_execute_command             = false
  propagate_tags                     = "SERVICE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.task_subnet_ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = var.assign_public_ip
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.router.arn
    container_name   = "router"
    container_port   = local.router_port
  }

  depends_on = [aws_lb_listener.https]
}
