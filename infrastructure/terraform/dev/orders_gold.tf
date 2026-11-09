
# CommerceFlow - Orders Silver to Gold

data "aws_iam_role" "orders_gold_glue_role" {
  name = "AWSGlueServiceRole-${local.name_prefix}"
}

resource "aws_s3_object" "orders_to_gold_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/orders_to_gold.py"

  source = "${path.module}/../../../pipelines/batch/gold/orders_to_gold.py"
  etag   = filemd5("${path.module}/../../../pipelines/batch/gold/orders_to_gold.py")
}

resource "aws_iam_role_policy" "orders_gold_s3" {
  name = "${local.name_prefix}-orders-gold-s3"
  role = data.aws_iam_role.orders_gold_glue_role.name

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid      = "ListLake"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Sid    = "ReadSilverOrdersAndScript"
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = [
          "${aws_s3_bucket.data_lake.arn}/silver/orders/*",
          "${aws_s3_bucket.data_lake.arn}/scripts/orders_to_gold.py"
        ]
      },
      {
        Sid    = "WriteGoldFactOrders"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]

        Resource = "${aws_s3_bucket.data_lake.arn}/gold/fact_orders/*"
      }
    ]
  })
}

resource "aws_glue_job" "orders_to_gold" {
  name        = "${local.name_prefix}-orders-to-gold"
  description = "Transform Silver orders into Gold fact_orders"

  role_arn     = data.aws_iam_role.orders_gold_glue_role.arn
  glue_version = "5.1"

  worker_type       = "G.1X"
  number_of_workers = 2

  timeout     = 20
  max_retries = 0

  command {
    name           = "glueetl"
    python_version = "3"

    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.orders_to_gold_script.key}"
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

  depends_on = [
    aws_iam_role_policy.orders_gold_s3
  ]
}
