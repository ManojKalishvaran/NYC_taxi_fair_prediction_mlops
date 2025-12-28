resource "aws_cloudwatch_event_rule" "model_registered" {
  name = "model-registered-rule"

  event_pattern = jsonencode({
    source = ["aws.sagemaker"]
    "detail-type" = ["SageMaker Model Package State Change"]
    detail = {
      ModelPackageGroupName = ["NYCTaxiFareModels"]
      ModelApprovalStatus   = ["PendingManualApproval"]
    }
  })
}
resource "aws_cloudwatch_event_rule" "pipeline_failed" {
  name = "pipeline-failed-rule"

  event_pattern = jsonencode({
    source = ["aws.sagemaker"]
    "detail-type" = ["SageMaker Pipeline Execution Status Change"]
    detail = {
      PipelineExecutionStatus = ["Failed"]
    }
  })
}
