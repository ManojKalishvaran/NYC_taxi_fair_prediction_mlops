import json
import boto3
import os

sm = boto3.client("sagemaker", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

sns = boto3.client("sns", region_name="us-east-1")

API_BASE = os.environ["APPROVAL_API_BASE"]


import time

def fetch_metrics(model_package_arn, retries=5, delay=3):
    for attempt in range(retries):
        response = sm.describe_model_package(
            ModelPackageName=model_package_arn
        )

        model_stats = (
            response
            .get("ModelMetrics", {})
            .get("ModelStatistics")
        )

        if model_stats:
            s3_uri = model_stats["S3Uri"]
            bucket = s3_uri.split("/")[2]
            key = "/".join(s3_uri.split("/")[3:])
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read())

        time.sleep(delay)

    return None



SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):
    detail_type = event.get("detail-type")
    detail = event.get("detail", {})

    if detail_type == "SageMaker Model Package State Change":
        status = "SUCCESS"
        model_arn = detail.get("ModelPackageArn")
        metrics = fetch_metrics(model_arn)

        approve_link = f"{API_BASE}/approve?action=approve&modelPackageArn={model_arn}"
        reject_link  = f"{API_BASE}/reject?action=reject&modelPackageArn={model_arn}"

        approval = detail.get("ModelApprovalStatus")
        
        if metrics is None:
            metrics_block = "⚠️ Metrics not yet available. Please refresh in a few minutes."
        else:
            train = metrics["train_score"]
            test = metrics["test_score"]
            metrics_block = f"""

📊 Evaluation Metrics

TRAIN
- RMSE: {train['RMSE']:.2f}
- MAE : {train['MAE']:.2f}
- R2  : {train['R2']:.4f}

TEST
- RMSE: {test['RMSE']:.2f}
- MAE : {test['MAE']:.2f}
- R2  : {test['R2']:.4f}
"""
        message = f"""
✅ Model Registered Successfully

Model ARN:
{model_arn}

Approval Status:
{approval}

{metrics_block}

Next step:
Approve model to trigger deployment.

Approve: 
click - {approve_link}

Reject: 
click - {reject_link}
"""

    elif detail_type == "SageMaker Pipeline Execution Status Change":
        status = "FAILURE"
        pipeline_arn = detail.get("pipelineExecutionArn")

        message = f"""
❌ Pipeline Failed

Pipeline ARN:
{pipeline_arn}

Check CloudWatch logs for details.
"""

    else:
        return {"status": "ignored"}

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[MLOps] Model Pipeline {status}",
        Message=message
    )

    return {"status": "sent"}
