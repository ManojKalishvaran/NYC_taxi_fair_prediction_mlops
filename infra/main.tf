provider "aws" {
  region = var.aws_region
}

# -------------------------
# S3 bucket for RAW data
# -------------------------
resource "aws_s3_bucket" "raw_bucket" {
  bucket = var.raw_bucket_name
}

# -------------------------
# S3 bucket for PROCESSED data
# -------------------------
resource "aws_s3_bucket" "processed_bucket" {
  bucket = var.processed_bucket_name
}

# -------------------------
# IAM role for SageMaker
# -------------------------
resource "aws_iam_role" "sagemaker_execution_role" {
  name = "sagemaker_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "sagemaker.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# -------------------------
# IAM policy for S3 access
# -------------------------
resource "aws_iam_policy" "s3_access_policy" {
  name = "sagemaker_s3_access_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:HeadObject"
        ]
        Resource = [
          aws_s3_bucket.raw_bucket.arn,
          "${aws_s3_bucket.raw_bucket.arn}/*",
          aws_s3_bucket.processed_bucket.arn,
          "${aws_s3_bucket.processed_bucket.arn}/*"
        ]
      }
    ]
  })
}

# -------------------------
# Attach policy to role
# -------------------------
resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}
