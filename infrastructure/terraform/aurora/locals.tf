# Local project variables
locals {
  data_lake_bucket_name = "${var.project_name}-${var.environment}-lake-${var.aws_account_id}-${var.aws_region}"
}