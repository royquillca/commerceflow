
data "aws_iam_role" "payments_silver_role" {
  name = "AWSGlueServiceRole-${local.name_prefix}"
}

resource "aws_s3_object" "payments_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/payments_to_silver.py"
  source = "${path.root}/../../../pipelines/batch/silver/payments_to_silver.py"
  etag   = filemd5("${path.root}/../../../pipelines/batch/silver/payments_to_silver.py")
}

resource "aws_iam_role_policy" "payments_silver_s3" {
  name = "${local.name_prefix}-payments-silver-s3"
  role = data.aws_iam_role.payments_silver_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/bronze/payments/*",
          "${aws_s3_bucket.data_lake.arn}/scripts/payments_to_silver.py"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/silver/payments/*",
          "${aws_s3_bucket.data_lake.arn}/quarantine/payments/*"
        ]
      }
    ]
  })
}

resource "aws_glue_job" "payments_to_silver" {
  name              = "${local.name_prefix}-payments-to-silver"
  role_arn          = data.aws_iam_role.payments_silver_role.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.payments_silver_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--BUCKET_NAME"                      = aws_s3_bucket.data_lake.bucket
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_iam_role_policy.payments_silver_s3]
}
