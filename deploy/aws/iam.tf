# Two roles. The execution role is what ECS uses to start the task: write its
# logs and, if any secrets are configured, read those parameters. The task
# role is what the router itself runs as, and it holds exactly the two
# DynamoDB actions the history code calls, on one table. No UpdateItem,
# DeleteItem or Scan: history.rs is designed around that grant.

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "execution" {
  statement {
    sid       = "WriteRouterLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.router.arn}:*"]
  }

  dynamic "statement" {
    for_each = length(var.secret_parameter_arns) > 0 ? [1] : []

    content {
      sid       = "ReadRouterSecrets"
      actions   = ["ssm:GetParameters"]
      resources = sort(distinct(values(var.secret_parameter_arns)))
    }
  }

  dynamic "statement" {
    for_each = var.secret_kms_key_arn != null && length(var.secret_parameter_arns) > 0 ? [1] : []

    content {
      sid       = "DecryptRouterSecrets"
      actions   = ["kms:Decrypt"]
      resources = [var.secret_kms_key_arn]
    }
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "start-router-task"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

resource "aws_iam_role" "task" {
  name               = "${var.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid       = "RoomHistory"
    actions   = ["dynamodb:PutItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.room_history.arn]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "room-history"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}
