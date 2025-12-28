data "archive_file" "notify_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/notify_model_status"
  output_path = "${path.module}/notify_model_status.zip"
}
resource "aws_lambda_function" "notify_model_status" {
  function_name = "mlops-notify-model-status"
  runtime       = "python3.10"
  handler       = "handler.lambda_handler"
  role          = aws_iam_role.lambda_exec_role.arn

  filename         = data.archive_file.notify_lambda_zip.output_path
  source_code_hash = data.archive_file.notify_lambda_zip.output_base64sha256

  timeout = 30

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.mlops_notifications.arn
	  APPROVAL_API_BASE = "${aws_apigatewayv2_api.approval_api.api_endpoint}/prod"
    }
  }
}
resource "aws_cloudwatch_event_target" "success_target" {
  rule = aws_cloudwatch_event_rule.model_registered.name
  arn  = aws_lambda_function.notify_model_status.arn
}

resource "aws_cloudwatch_event_target" "failure_target" {
  rule = aws_cloudwatch_event_rule.pipeline_failed.name
  arn  = aws_lambda_function.notify_model_status.arn
}

resource "aws_lambda_permission" "allow_eventbridge_model_registered" {
  statement_id  = "AllowEventBridgeModelRegistered"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify_model_status.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.model_registered.arn
}
resource "aws_lambda_permission" "allow_eventbridge_pipeline_failed" {
  statement_id  = "AllowEventBridgePipelineFailed"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify_model_status.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.pipeline_failed.arn
}

