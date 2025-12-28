import boto3
import argparse
import time

sm = boto3.client("sagemaker")

def endpoint_exists(name):
    try:
        sm.describe_endpoint(EndpointName=name)
        return True
    except sm.exceptions.ClientError:
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-package-arn", required=True)
    parser.add_argument("--endpoint-name", required=True)
    parser.add_argument("--instance-type", required=True)
    parser.add_argument("--initial-instance-count", type=int, required=True)
    args = parser.parse_args()

    model_name = f"nyc-taxi-model-{int(time.time())}"
    endpoint_config_name = f"{args.endpoint_name}-config-{int(time.time())}"

    # 1️⃣ Create Model from Model Package
    sm.create_model(
        ModelName=model_name,
        ExecutionRoleArn=sm.describe_model_package(
            ModelPackageName=args.model_package_arn
        )["InferenceSpecification"]["Containers"][0]["ModelPackageName"],
        Containers=[
            {"ModelPackageName": args.model_package_arn}
        ],
    )

    # 2️⃣ Create EndpointConfig
    sm.create_endpoint_config(
        EndpointConfigName=endpoint_config_name,
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": model_name,
                "InstanceType": args.instance_type,
                "InitialInstanceCount": args.initial_instance_count,
            }
        ],
    )

    # 3️⃣ Create or Update Endpoint
    if endpoint_exists(args.endpoint_name):
        sm.update_endpoint(
            EndpointName=args.endpoint_name,
            EndpointConfigName=endpoint_config_name,
        )
    else:
        sm.create_endpoint(
            EndpointName=args.endpoint_name,
            EndpointConfigName=endpoint_config_name,
        )

    print("Deployment triggered successfully.")

if __name__ == "__main__":
    main()
