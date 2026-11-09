
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambdas/aurora_test"
  output_path = "${path.module}/aurora_test.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-${var.environment}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_aurora" {
  name = "aurora-data-api-access"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "rds-data:ExecuteStatement",
          "rds-data:BatchExecuteStatement",
          "rds-data:BeginTransaction",
          "rds-data:CommitTransaction",
          "rds-data:RollbackTransaction"
        ]
        Resource = aws_rds_cluster.aurora.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_rds_cluster.aurora.master_user_secret[0].secret_arn
      }
    ]
  })
}

resource "aws_lambda_function" "aurora_test" {
  function_name = "${var.project_name}-${var.environment}-aurora-test"

  role    = aws_iam_role.lambda.arn
  runtime = "python3.12"
  handler = "handler.lambda_handler"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  timeout     = 180
  memory_size = 256

  environment {
    variables = {
      CLUSTER_ARN   = aws_rds_cluster.aurora.arn
      SECRET_ARN    = aws_rds_cluster.aurora.master_user_secret[0].secret_arn
      DATABASE_NAME = var.database_name
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda_aurora,
    aws_iam_role_policy_attachment.lambda_logs
  ]
}
