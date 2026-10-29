# 1. VIRTUAL PRIVATE CLOUD

resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  enable_dns_support   = true
  enable_dns_hostnames = true

  instance_tenancy = "default"

  tags = {
    Name = "${var.project_name}-${var.environment}-vpc"
  }
}


# 2. AVAILABILITY ZONES

data "aws_availability_zones" "available" {
  state = "available"
}


# 3. PRIVATE SUBNETS

resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id = aws_vpc.main.id

  cidr_block = var.private_subnet_cidrs[count.index]

  availability_zone = data.aws_availability_zones.available.names[
    count.index
  ]

  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-${var.environment}-private-${count.index + 1}"
  }
}


# 4. DATABASE SUBNET GROUP

locals {
  db_subnet_group_name   = "${var.project_name}-${var.environment}-db-subnets"
  db_security_group_name = "${var.project_name}-${var.environment}-aurora-sg"
}

resource "aws_db_subnet_group" "aurora" {
  name = local.db_subnet_group_name

  description = "Private subnets for Aurora PostgreSQL"

  subnet_ids = aws_subnet.private[*].id

  tags = {
    Name = local.db_subnet_group_name
  }
}


# 5. SECURITY GROUP

resource "aws_security_group" "aurora" {
  name        = local.db_security_group_name
  description = "Security group for Aurora PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress = []
  egress  = []

  tags = {
    Name = local.db_security_group_name
  }
}