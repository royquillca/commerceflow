# Databa in AWS Aurora

output "aurora_endpoint" {
  description = "Aurora writer endpoint."
  value       = aws_rds_cluster.aurora.endpoint
}

output "aurora_port" {
  description = "PostgreSQL connection port."
  value       = aws_rds_cluster.aurora.port
}

output "database_name" {
  description = "Initial PostgreSQL database."
  value       = aws_rds_cluster.aurora.database_name
}

output "aurora_secret_arn" {
  description = "ARN of the master credentials in Secrets Manager."
  value       = aws_rds_cluster.aurora.master_user_secret[0].secret_arn
}

output "vpc_id" {
  description = "VPC identifier."
  value       = aws_vpc.main.id
}

output "aurora_security_group_id" {
  description = "Aurora security group identifier."
  value       = aws_security_group.aurora.id
}


# Data Lake in AWS S3

output "data_lake_bucket_name" {
  description = "S3 bucket used by CommerceFlow data lake."
  value       = aws_s3_bucket.data_lake.id
}