
# CommerceFlow - EventBridge Scheduler
# Automatically starts the Step Functions pipeline

# --------------------------------------------------
# Configuration
# --------------------------------------------------

variable "commerceflow_schedule_expression" {
  description = "Execution frequency for CommerceFlow"
  type        = string
  # Start execution at 22:00, every day
  default = "cron(0 22 * * ? *)"
}

variable "commerceflow_schedule_enabled" {
  description = "Enable automatic pipeline executions"
  type        = bool
  # default     = true
  default = false
}

# --------------------------------------------------
# IAM Role - EventBridge Scheduler
# --------------------------------------------------

resource "aws_iam_role" "commerceflow_scheduler" {
  name = "${local.name_prefix}-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Effect = "Allow"

      Principal = {
        Service = "scheduler.amazonaws.com"
      }

      Action = "sts:AssumeRole"
    }]
  })
}

# --------------------------------------------------
# Permission to start Step Functions
# --------------------------------------------------

resource "aws_iam_role_policy" "commerceflow_scheduler" {
  name = "${local.name_prefix}-scheduler-policy"
  role = aws_iam_role.commerceflow_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Sid    = "StartCommerceFlowPipeline"
      Effect = "Allow"

      Action = [
        "states:StartExecution"
      ]

      Resource = aws_sfn_state_machine.commerceflow_batch.arn
    }]
  })
}

# --------------------------------------------------
# EventBridge Schedule
# --------------------------------------------------

resource "aws_scheduler_schedule" "commerceflow_batch" {
  name        = "${local.name_prefix}-daily-schedule"
  description = "Scheduled execution of CommerceFlow ETL pipeline"

  group_name = "default"

  schedule_expression = var.commerceflow_schedule_expression

  schedule_expression_timezone = "America/Lima"

  state = var.commerceflow_schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.commerceflow_batch.arn
    role_arn = aws_iam_role.commerceflow_scheduler.arn

    # Default execution: Aurora -> Bronze -> Silver -> Gold
    input = jsonencode({
      start_from = "full"
    })

    retry_policy {
      maximum_retry_attempts = 0
    }
  }

  depends_on = [
    aws_iam_role_policy.commerceflow_scheduler
  ]
}
