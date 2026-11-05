# AWS CONFIGURATION

variable "aws_region" {
  description = "AWS region where resources will be deployed"
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account allowed to deploy this infraestructure"
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "AWS account must be contain exactly 12 digits."
  }
}


# PROJECT CONFIGURATION

variable "project_name" {
  description = "Project name used in resource names and tags."
  type        = string
  default     = "CommerceFlow"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Use dev, staging or prod."
  }
}

# AURORA CONFIGURATION

variable "database_name" {
  description = "Initial PostgreSQL database name."
  type        = string
  default     = "commerceflow"
}

variable "master_username" {
  description = "Aurora master database username."
  type        = string
  default     = "postgres"
}

variable "engine_version" {
  description = "Aurora PostgreSQL engine version."
  type        = string
  default     = "16.4"
}

variable "min_capacity" {
  description = "Minimum Aurora Serverless v2 capacity in ACUs."
  type        = number
  default     = 0
}

variable "max_capacity" {
  description = "Maximum Aurora Serverless v2 capacity in ACUs."
  type        = number
  default     = 2
}

variable "auto_pause_seconds" {
  description = "Inactivity period before Aurora automatically pauses."
  type        = number
  default     = 600
}


# NETWORK CONFIGURATION

variable "vpc_cidr" {
  description = "IPv4 CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for the two private subnets."
  type        = list(string)
  default     = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "database_port" {
  description = "PostgreSQL TCP port."
  type        = number
  default     = 5432

  validation {
    condition     = var.database_port >= 1 && var.database_port <= 65535
    error_message = "Port must be between 1 and 65535."
  }
}
