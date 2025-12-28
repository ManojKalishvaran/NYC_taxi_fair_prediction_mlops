import argparse
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.processing import ScriptProcessor
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline_context import PipelineSession

# ---------------------------------------------------------------------
# ARGUMENTS
# ---------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="CI mode: validate deployment pipeline definition only"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="CD/orchestration mode: upsert deployment pipeline"
    )
    return parser.parse_args()

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
ROLE = "arn:aws:iam::818831377059:role/sagemaker-execution-role"
REGION = "us-east-1"

session = PipelineSession()

# ---------------------------------------------------------------------
# PIPELINE PARAMETERS
# ---------------------------------------------------------------------
model_package_arn = ParameterString(
    name="ModelPackageArn"
)

endpoint_name = ParameterString(
    name="EndpointName",
    default_value="nyc-taxi-fare-endpoint"
)

# ---------------------------------------------------------------------
# DEPLOYMENT STEP
# ---------------------------------------------------------------------
deploy_processor = ScriptProcessor(
    image_uri=sagemaker.image_uris.retrieve(
        framework="sklearn",
        region=REGION,
        version="1.2-1",
        py_version="py3",
        instance_type="ml.t3.xlarge",
    ),
    command=["python3"],
    role=ROLE,
    instance_type="ml.t3.xlarge",
    instance_count=1,
    sagemaker_session=session,
)

deploy_step = ProcessingStep(
    name="DeployEndpoint",
    processor=deploy_processor,
    code="src/deployment/deploy_endpoint.py",
    job_arguments=[
        "--model-package-arn", model_package_arn,
        "--endpoint-name", endpoint_name,
        "--instance-type", "ml.t3.xlarge",
        "--initial-instance-count", "1",
    ],
)

# ---------------------------------------------------------------------
# PIPELINE FACTORY
# ---------------------------------------------------------------------
def build_pipeline() -> Pipeline:
    return Pipeline(
        name="NYCTaxiDeployPipeline",
        parameters=[model_package_arn, endpoint_name],
        steps=[deploy_step],
        sagemaker_session=session,
    )

# ---------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------
if __name__ == "__main__":
    args = parse_args()

    if args.validate_only:
        build_pipeline()
        print("✅ Deployment pipeline definition validated (CI mode)")

    elif args.execute:
        pipeline = build_pipeline()
        pipeline.upsert(role_arn=ROLE)
        print("🚀 Deployment pipeline upserted successfully")

    else:
        raise RuntimeError(
            "You must specify --validate-only (CI) or --execute (CD/orchestration)"
        )
