import json
import boto3

sm = boto3.client("sagemaker", region_name="us-east-1")

def lambda_handler(event, context):
    # API Gateway (HTTP API) passes query params here
    params = event.get("queryStringParameters") or {}

    model_arn = params.get("modelPackageArn")
    action = params.get("action")  # approve | reject

    if not model_arn or action not in ["approve", "reject"]:
        return {
            "statusCode": 400,
            "body": "Invalid request"
        }

    approval_status = (
        "Approved" if action == "approve" else "Rejected"
    )

    sm.update_model_package(
        ModelPackageArn=model_arn,
        ModelApprovalStatus=approval_status
    )

    return {
        "statusCode": 200,
        "body": f"Model {approval_status} successfully"
    }
