resource "aws_iam_role" "lambda_exec_role" {
  name = "mlops-notify-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}	

resource "aws_iam_role_policy" "lambda_policy" {
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # CloudWatch logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },

      # SNS email
      {
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = aws_sns_topic.mlops_notifications.arn
      },

      # 🔑 REQUIRED: read model registry metadata
      {
        Effect = "Allow"
        Action = [
          "sagemaker:DescribeModelPackage"
        ]
        Resource = "*"
      },

      # 🔑 REQUIRED: read evaluation.json from S3
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "*"
      }
    ]
  })
}

