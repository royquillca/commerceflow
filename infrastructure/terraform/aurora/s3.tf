# 1. DATA LAKE BUCKET
resource "aws_s3_bucket" "data_lake" {
  bucket        = local.data_lake_bucket_name
  force_destroy = false

  tags = {
    Name = local.data_lake_bucket_name
  }
}

# 2. BLOCK ALL PUBLIC ACCESS
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. ENCRYPTION AT REST
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 4. BUCKET OWNERSHIP
resource "aws_s3_bucket_ownership_controls" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# 5. DISABLE VERSIONING FOR THE DEVELOPMENT LAKE
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Suspended"
  }
}
