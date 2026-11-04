# Local project variables
locals {
  name_prefix = "${var.project_name}-${var.environment}"

  data_lake_bucket_name = "${var.project_name}-${var.environment}-lake-${var.aws_account_id}-${var.aws_region}"
}