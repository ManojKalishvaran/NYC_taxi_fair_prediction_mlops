import boto3
import os

sm = boto3.client("sagemaker")

DEPLOY_PIPELINE_NAME = os.environ["DEPLOY_PIPELINE_NAME"]
ENDPOINT_NAME = os.environ["ENDPOINT_NAME"]

def lambda_handler(event, context):
    detail = event["detail"]

    model_package_arn = detail["ModelPackageArn"]

    sm.start_pipeline_execution(
        PipelineName=DEPLOY_PIPELINE_NAME,
        PipelineParameters=[
            {
                "Name": "ModelPackageArn",
                "Value": model_package_arn
            },
            {
                "Name": "EndpointName",
                "Value": ENDPOINT_NAME
            }
        ]
    )

    return {"status": "deployment triggered"}
