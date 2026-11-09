
# ============================================================
# CommerceFlow - AWS Step Functions
# Stage 0: Wake up Aurora
# Stage 1: Aurora -> S3 Bronze (parallel ingestion)
# ============================================================

# ------------------------------------------------------------
# Data sources
# ------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_lambda_function" "aurora_readiness" {
  function_name = "${local.name_prefix}-aurora-test"
}

# ------------------------------------------------------------
# IAM Role - Step Functions
# ------------------------------------------------------------

resource "aws_iam_role" "commerceflow_sfn" {
  name = "${local.name_prefix}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "states.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}

# ------------------------------------------------------------
# IAM Policy - Lambda and Glue
# ------------------------------------------------------------

resource "aws_iam_role_policy" "commerceflow_sfn" {
  name = "${local.name_prefix}-step-functions-policy"
  role = aws_iam_role.commerceflow_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "InvokeAuroraReadiness"
        Effect = "Allow"

        Action = [
          "lambda:InvokeFunction"
        ]

        Resource = data.aws_lambda_function.aurora_readiness.arn
      },
      # Policy to run Bronze Glue Job
      {
        Sid    = "RunAndMonitorBronze"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:job/${local.name_prefix}-aurora-to-bronze",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:job/${local.name_prefix}-*-to-silver",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:job/${local.name_prefix}-*-to-gold"
        ]
      }
    ]
  })
}

# ------------------------------------------------------------
# State Machine
# ------------------------------------------------------------

resource "aws_sfn_state_machine" "commerceflow_batch" {
  name     = "${local.name_prefix}-batch-pipeline"
  role_arn = aws_iam_role.commerceflow_sfn.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "CommerceFlow: Aurora -> Bronze"
    # StartAt = "WakeUpAurora"
    StartAt = "RoutePipeline"

    States = {
      # Choice where to init execution
      RoutePipeline = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.start_from"
            StringEquals = "silver"
            Next         = "PrepareSilver"
          },
          {
            Variable     = "$.start_from"
            StringEquals = "gold"
            Next         = "PrepareGold"
          },
          {
            Variable     = "$.start_from"
            StringEquals = "full"
            Next         = "WakeUpAurora"
          },
          {
            Variable  = "$.start_from"
            IsPresent = false
            Next      = "WakeUpAurora"
          }
        ]
        Default = "InvalidStartStage"
      }
      InvalidStartStage = {
        Type  = "Fail"
        Error = "InvalidStartStage"
        Cause = "Use full, silver or gold as start_from"
      }

      # ------------------------------------------------------
      # Stage 0 - Wake up Aurora
      # ------------------------------------------------------

      WakeUpAurora = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"

        Parameters = {
          FunctionName = data.aws_lambda_function.aurora_readiness.arn

          Payload = {
            action = "test"
          }
        }

        ResultPath = "$.aurora_test"

        # Retry = [
        #   {
        #     ErrorEquals = [
        #       "States.TaskFailed"
        #     ]

        #     IntervalSeconds = 10
        #     MaxAttempts     = 3
        #     BackoffRate     = 2
        #   }
        # ]

        Next = "CheckAuroraReadiness"
      }

      # ------------------------------------------------------
      # Validate Aurora response
      # ------------------------------------------------------

      CheckAuroraReadiness = {
        Type = "Choice"

        Choices = [
          {
            And = [
              {
                Variable      = "$.aurora_test.Payload.statusCode"
                NumericEquals = 200
              },
              {
                Variable      = "$.aurora_test.Payload.result"
                NumericEquals = 1
              }
            ]

            Next = "PrepareBronze"
          }
        ]

        Default = "AuroraNotReady"
      }

      AuroraNotReady = {
        Type  = "Fail"
        Error = "AuroraReadinessFailed"
        Cause = "Aurora did not return a successful SQL result"
      }

      # ------------------------------------------------------
      # Stage 1 - Prepare Bronze tables
      # ------------------------------------------------------

      PrepareBronze = {
        Type = "Pass"

        Result = [
          "customers",
          "products",
          "orders",
          "order_items",
          "payments"
        ]

        ResultPath = "$.bronze_tables"
        Next       = "BronzeIngestion"
      }

      # ------------------------------------------------------
      # Stage 1 - Parallel Aurora -> Bronze ingestion
      # ------------------------------------------------------

      BronzeIngestion = {
        Type           = "Map"
        ItemsPath      = "$.bronze_tables"
        MaxConcurrency = 2

        # Preserve the original input.
        # Do not accumulate Glue job outputs.
        ResultPath = null

        ItemSelector = {
          "table.$" = "$$.Map.Item.Value"
        }

        ItemProcessor = {
          ProcessorConfig = {
            Mode = "INLINE"
          }

          StartAt = "RunAuroraToBronze"

          States = {
            RunAuroraToBronze = {
              Type     = "Task"
              Resource = "arn:aws:states:::glue:startJobRun.sync"

              Parameters = {
                JobName = "${local.name_prefix}-aurora-to-bronze"

                Arguments = {
                  "--SOURCE_TABLE.$" = "$.table"
                }
              }

              End = true
            }
          }
        }

        Next = "PrepareSilver"
      }


      # ------------------------------------------------------
      # Stage 2 - Prepare Silver Glue Jobs
      # ------------------------------------------------------

      PrepareSilver = {
        Type = "Pass"

        Result = [
          "${local.name_prefix}-customers-to-silver",
          "${local.name_prefix}-products-to-silver",
          "${local.name_prefix}-orders-to-silver",
          "${local.name_prefix}-order-items-to-silver",
          "${local.name_prefix}-payments-to-silver"
        ]

        ResultPath = "$.silver_jobs"
        Next       = "SilverTransformations"
      }

      # ------------------------------------------------------
      # Stage 2 - Execute Silver jobs in parallel
      # ------------------------------------------------------

      SilverTransformations = {
        Type           = "Map"
        ItemsPath      = "$.silver_jobs"
        MaxConcurrency = 3
        ResultPath     = null

        ItemSelector = {
          "job_name.$" = "$$.Map.Item.Value"
        }

        ItemProcessor = {
          ProcessorConfig = {
            Mode = "INLINE"
          }

          StartAt = "RunSilverJob"

          States = {
            RunSilverJob = {
              Type     = "Task"
              Resource = "arn:aws:states:::glue:startJobRun.sync"

              Parameters = {
                "JobName.$" = "$.job_name"
              }

              End = true
            }
          }
        }

        Next = "PrepareGold"
      }

      # ------------------------------------------------------
      # Stage 3 - Prepare Gold Glue Jobs
      # ------------------------------------------------------

      PrepareGold = {
        Type = "Pass"

        Result = [
          "${local.name_prefix}-customers-to-gold",
          "${local.name_prefix}-products-to-gold",
          "${local.name_prefix}-orders-to-gold",
          "${local.name_prefix}-order-items-to-gold",
          "${local.name_prefix}-payments-to-gold"
        ]

        ResultPath = "$.gold_jobs"
        Next       = "GoldTransformations"
      }

      # ------------------------------------------------------
      # Stage 3 - Silver -> Gold
      # ------------------------------------------------------

      GoldTransformations = {
        Type           = "Map"
        ItemsPath      = "$.gold_jobs"
        MaxConcurrency = 3
        ResultPath     = null

        ItemSelector = {
          "job_name.$" = "$$.Map.Item.Value"
        }

        ItemProcessor = {
          ProcessorConfig = {
            Mode = "INLINE"
          }

          StartAt = "RunGoldJob"

          States = {
            RunGoldJob = {
              Type     = "Task"
              Resource = "arn:aws:states:::glue:startJobRun.sync"

              Parameters = {
                "JobName.$" = "$.job_name"
              }

              End = true
            }
          }
        }

        Next = "GoldCompleted"
      }
      # End CommerceFlow pipeline
      GoldCompleted = {
        Type = "Succeed"
      }
    }

  })

  depends_on = [
    aws_iam_role_policy.commerceflow_sfn
  ]
}
