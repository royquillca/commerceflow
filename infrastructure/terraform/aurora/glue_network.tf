
# Security group for AWS Glue
resource "aws_security_group" "glue" {
  name   = "${var.project_name}-${var.environment}-glue-sg"
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-glue-sg"
  }
}

# Spark workers communicate with each other
resource "aws_vpc_security_group_ingress_rule" "glue_self" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.glue.id

  ip_protocol = "tcp"
  from_port   = 0
  to_port     = 65535
}

resource "aws_vpc_security_group_egress_rule" "glue_self" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.glue.id

  ip_protocol = "tcp"
  from_port   = 0
  to_port     = 65535
}

# Glue can connect to Aurora
resource "aws_vpc_security_group_ingress_rule" "glue_to_aurora" {
  security_group_id            = aws_security_group.aurora.id
  referenced_security_group_id = aws_security_group.glue.id

  ip_protocol = "tcp"
  from_port   = var.database_port
  to_port     = var.database_port
}

resource "aws_vpc_security_group_egress_rule" "glue_to_aurora" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.aurora.id

  ip_protocol = "tcp"
  from_port   = var.database_port
  to_port     = var.database_port
}

# Private S3 access without a NAT Gateway
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = [
    aws_vpc.main.default_route_table_id
  ]

  tags = {
    Name = "${var.project_name}-${var.environment}-s3-endpoint"
  }
}

# Permit Glue to access S3 through the endpoint
resource "aws_vpc_security_group_egress_rule" "glue_to_s3" {
  security_group_id = aws_security_group.glue.id

  ip_protocol    = "tcp"
  from_port      = 443
  to_port        = 443
  prefix_list_id = aws_vpc_endpoint.s3.prefix_list_id
}

# Secrets Manager private endpoint
resource "aws_security_group" "secrets_endpoint" {
  name   = "${var.project_name}-${var.environment}-secrets-endpoint-sg"
  vpc_id = aws_vpc.main.id
}

resource "aws_vpc_security_group_ingress_rule" "secrets_https" {
  security_group_id            = aws_security_group.secrets_endpoint.id
  referenced_security_group_id = aws_security_group.glue.id

  ip_protocol = "tcp"
  from_port   = 443
  to_port     = 443
}

resource "aws_vpc_security_group_egress_rule" "glue_to_secrets" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.secrets_endpoint.id

  ip_protocol = "tcp"
  from_port   = 443
  to_port     = 443
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true

  subnet_ids = [
    aws_subnet.private[0].id
  ]

  security_group_ids = [
    aws_security_group.secrets_endpoint.id
  ]

  tags = {
    Name = "${var.project_name}-${var.environment}-secrets-endpoint"
  }
}
