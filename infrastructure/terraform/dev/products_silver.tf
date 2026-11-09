
# CommerceFlow - Products Bronze to Silver

data "aws_iam_role" "products_silver_glue_role" {
  name = "AWSGlueServiceRole-${local.name_prefix}"
}

resource "aws_s3_object" "products_to_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/products_to_silver.py"

  source = "${path.root}/../../../pipelines/batch/silver/products_to_silver.py"
  etag   = filemd5("${path.root}/../../../pipelines/batch/silver/products_to_silver.py")
}

resource "aws_iam_role_policy" "products_silver_s3" {
  name = "${local.name_prefix}-products-silver-s3"
  role = data.aws_iam_role.products_silver_glue_role.name

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid      = "ListDataLake"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Sid    = "ReadBronzeProductsAndScript"
        Effect = "Allow"

        Action = ["s3:GetObject"]

        Resource = [
          "${aws_s3_bucket.data_lake.arn}/bronze/products/*",
          "${aws_s3_bucket.data_lake.arn}/scripts/products_to_silver.py"
        ]
      },
      {
        Sid    = "WriteSilverProductsAndQuarantine"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]

        Resource = [
          "${aws_s3_bucket.data_lake.arn}/silver/products/*",
          "${aws_s3_bucket.data_lake.arn}/quarantine/products/*"
        ]
      }
    ]
  })
}

resource "aws_glue_job" "products_to_silver" {
  name        = "${local.name_prefix}-products-to-silver"
  description = "Validate and transform Bronze products to Silver"

  role_arn     = data.aws_iam_role.products_silver_glue_role.arn
  glue_version = "5.1"

  worker_type       = "G.1X"
  number_of_workers = 2

  timeout     = 20
  max_retries = 0

  command {
    name           = "glueetl"
    python_version = "3"

    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.products_to_silver_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--BUCKET_NAME"                      = aws_s3_bucket.data_lake.bucket
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_iam_role_policy.products_silver_s3
  ]
}
