import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.processing import ScriptProcessor
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline_context import PipelineSession

ROLE = "arn:aws:iam::818831377059:role/sagemaker-execution-role"
REGION = "us-east-1"

session = PipelineSession()

model_package_arn = ParameterString(name="ModelPackageArn")
endpoint_name = ParameterString(
    name="EndpointName",
    default_value="nyc-taxi-fare-endpoint"
)

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

pipeline = Pipeline(
    name="NYCTaxiDeployPipeline",
    parameters=[model_package_arn, endpoint_name],
    steps=[deploy_step],
    sagemaker_session=session,
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=ROLE)
