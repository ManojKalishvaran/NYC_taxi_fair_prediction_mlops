resource "aws_apigatewayv2_api" "approval_api" {
  name          = "mlops-approval-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "approve_lambda" {
  api_id             = aws_apigatewayv2_api.approval_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.approve_model.invoke_arn
  integration_method = "POST"
}

resource "aws_apigatewayv2_route" "approve_route" {
  api_id    = aws_apigatewayv2_api.approval_api.id
  route_key = "GET /approve"
  target    = "integrations/${aws_apigatewayv2_integration.approve_lambda.id}"
}

resource "aws_apigatewayv2_route" "reject_route" {
  api_id    = aws_apigatewayv2_api.approval_api.id
  route_key = "GET /reject"
  target    = "integrations/${aws_apigatewayv2_integration.approve_lambda.id}"
}

resource "aws_lambda_permission" "allow_api_gateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.approve_model.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.approval_api.execution_arn}/*/*"
}

resource "aws_apigatewayv2_stage" "prod" {
  api_id      = aws_apigatewayv2_api.approval_api.id
  name        = "prod"
  auto_deploy = true
}
