data "archive_file" "approve_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/approve_model"
  output_path = "${path.module}/approve_model.zip"
}

resource "aws_lambda_function" "approve_model" {
  function_name = "mlops-approve-model"
  runtime       = "python3.10"
  handler       = "handler.lambda_handler"
  role          = aws_iam_role.lambda_exec_role.arn

  filename         = data.archive_file.approve_lambda_zip.output_path
  source_code_hash = data.archive_file.approve_lambda_zip.output_base64sha256
}
