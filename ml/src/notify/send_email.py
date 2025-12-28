import argparse
import boto3
import urllib.parse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", required=True)
    parser.add_argument("--rmse", required=True)
    parser.add_argument("--model_package_arn", required=False)
    return parser.parse_args()

def main():
    args = parse_args()

    sns = boto3.client("sns", region_name="us-east-1")
    topic_arn = "arn:aws:sns:us-east-1:818831377059:update_email_mlops"

    if args.status == "SUCCESS":
        encoded_arn = urllib.parse.quote(args.model_package_arn, safe="")

        deploy_link = (
            f"https://YOUR_API_GATEWAY_URL/deploy?"
            f"model_package_arn={encoded_arn}"
        )

        ignore_link = (
            f"https://YOUR_API_GATEWAY_URL/ignore?"
            f"model_package_arn={encoded_arn}"
        )

        message = f"""
NYC Taxi Fare Model Training Completed Successfully

RMSE: {args.rmse}

Model Package ARN:
{args.model_package_arn}

Actions:
Deploy Model: {deploy_link}
Ignore Model: {ignore_link}
"""
        subject = "🚀 NYC Taxi Model Ready for Deployment"

    else:
        message = f"NYC Taxi pipeline FAILED. RMSE={args.rmse}"
        subject = "❌ NYC Taxi Model Training Failed"

    sns.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject,
    )

if __name__ == "__main__":
    main()
