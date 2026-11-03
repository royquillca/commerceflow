
# 1. IAM ROLE

resource "aws_iam_role" "glue" {
  name = "AWSGlueServiceRole-${var.project_name}-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 2. ACCESS TO S3 AND SECRETS MANAGER

resource "aws_iam_role_policy" "glue_data" {
  name = "commerceflow-data-access"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "${aws_s3_bucket.data_lake.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_rds_cluster.aurora.master_user_secret[0].secret_arn
      }
    ]
  })
}

# 3. JDBC CONNECTION

resource "aws_glue_connection" "aurora" {
  name            = "${var.project_name}-${var.environment}-aurora-jdbc"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${aws_rds_cluster.aurora.endpoint}:${var.database_port}/${var.database_name}"
    SECRET_ID           = aws_rds_cluster.aurora.master_user_secret[0].secret_arn
    JDBC_ENFORCE_SSL    = "true"
  }

  physical_connection_requirements {
    availability_zone = aws_subnet.private[0].availability_zone
    subnet_id         = aws_subnet.private[0].id

    security_group_id_list = [
      aws_security_group.glue.id
    ]
  }

  depends_on = [
    aws_vpc_endpoint.secretsmanager
  ]
}

# 4. UPLOAD PYSPARK SCRIPT

resource "aws_s3_object" "aurora_to_s3_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/aurora_to_s3.py"

  source = "${path.module}/../../../pipelines/batch/bronze/aurora_to_s3.py"

  etag = filemd5(
    "${path.module}/../../../pipelines/batch/bronze/aurora_to_s3.py"
  )
}

# 5. PYSPARK GLUE JOB

resource "aws_glue_job" "aurora_to_bronze" {
  name     = "${var.project_name}-${var.environment}-aurora-to-bronze"
  role_arn = aws_iam_role.glue.arn

  glue_version = "5.1"
  worker_type  = "G.1X"

  number_of_workers = 2
  timeout           = 10
  max_retries       = 0

  connections = [
    aws_glue_connection.aurora.name
  ]

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.aurora_to_s3_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--BUCKET_NAME"                      = aws_s3_bucket.data_lake.id
    "--CONNECTION_NAME"                  = aws_glue_connection.aurora.name
    "--SOURCE_TABLE"                     = "orders"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_data,
    aws_s3_object.aurora_to_s3_script,
    aws_vpc_endpoint.s3
  ]
}
