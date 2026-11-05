# 1. AURORA POSTGRESQL CLUSTER

resource "aws_rds_cluster" "aurora" {
  cluster_identifier = "${var.project_name}-${var.environment}"

  # Database engine
  engine         = "aurora-postgresql"
  engine_mode    = "provisioned"
  engine_version = var.engine_version

  # Database credentials
  database_name               = var.database_name
  master_username             = var.master_username
  manage_master_user_password = true

  # Network and security
  db_subnet_group_name   = aws_db_subnet_group.aurora.name
  vpc_security_group_ids = [aws_security_group.aurora.id]
  port                   = var.database_port

  storage_encrypted = true
  storage_type      = "aurora"

  enable_http_endpoint = true

  # Backups and deletion
  backup_retention_period = 1
  copy_tags_to_snapshot   = true

  deletion_protection = false
  skip_final_snapshot = true

  # Serverless v2 scaling
  serverlessv2_scaling_configuration {
    min_capacity             = var.min_capacity
    max_capacity             = var.max_capacity
    seconds_until_auto_pause = var.auto_pause_seconds
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-cluster"
  }
}

# 2. AURORA WRITER INSTANCE

resource "aws_rds_cluster_instance" "writer" {
  identifier         = "${var.project_name}-${var.environment}-writer"
  cluster_identifier = aws_rds_cluster.aurora.id

  engine         = aws_rds_cluster.aurora.engine
  engine_version = aws_rds_cluster.aurora.engine_version

  instance_class = "db.serverless"

  publicly_accessible = false

  auto_minor_version_upgrade = true

  monitoring_interval          = 0
  performance_insights_enabled = false

  tags = {
    Name = "${var.project_name}-${var.environment}-writer"
  }
}
