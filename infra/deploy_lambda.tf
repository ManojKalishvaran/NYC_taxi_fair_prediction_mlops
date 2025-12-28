
data "archive_file" "deploy_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/trigger_deploy"
  output_path = "${path.module}/trigger_deploy.zip"
}

resource "aws_lambda_function" "trigger_deploy" {
  function_name = "mlops-trigger-deploy"
  runtime       = "python3.10"
  handler       = "handler.lambda_handler"
  role          = aws_iam_role.lambda_exec_role.arn

  filename         = data.archive_file.deploy_lambda_zip.output_path
  source_code_hash = data.archive_file.deploy_lambda_zip.output_base64sha256

  environment {
    variables = {
      DEPLOY_PIPELINE_NAME = "NYCTaxiDeployPipeline"
      ENDPOINT_NAME        = "nyc-taxi-fare-endpoint"
    }
  }
}
