import argparse
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import TrainingStep, ProcessingStep
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.workflow.parameters import ParameterString, ParameterInteger
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.functions import JsonGet, Join
from sagemaker.inputs import TrainingInput
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.model_step import ModelStep
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.sklearn.model import SKLearnModel

from pipeline_config import RAW_BUCKT, ROLE

# ---------------------------------------------------------------------
# ARGUMENTS
# ---------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="CI mode: validate pipeline definition only"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="CD/orchestration mode: upsert pipeline"
    )
    return parser.parse_args()

# ---------------------------------------------------------------------
# SESSION & PARAMETERS
# ---------------------------------------------------------------------
session = PipelineSession()
role = ROLE

unified_bucket = ParameterString(
    name="UnifiedBucket",
    default_value=RAW_BUCKT
)

n_estimators = ParameterInteger(
    name="n_estimators",
    default_value=200
)

# ---------------------------------------------------------------------
# PROCESSING STEP
# ---------------------------------------------------------------------
processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_type="ml.t3.xlarge",
    instance_count=1,
    base_job_name="nyc-taxi-preprocess",
    sagemaker_session=session,
)

processing_step = ProcessingStep(
    name="NYCTaxiPreprocessing",
    processor=processor,
    inputs=[
        ProcessingInput(
            source=Join(on="/", values=["s3:/", unified_bucket, "data/raw/v1"]),
            destination="/opt/ml/processing/input",
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/output",
            output_name="processed",
            destination=Join(on="/", values=["s3:/", unified_bucket, "data/processed/v1"]),
        )
    ],
    code="src/preprocessing/load_data.py",
    job_arguments=[
        "--input_file_path", "/opt/ml/processing/input/yellow_tripdata_v1.parquet",
        "--output_train_file_path", "/opt/ml/processing/output/train.csv",
        "--output_test_file_path", "/opt/ml/processing/output/test.csv",
        "--target", "fare_amount",
    ],
)

# ---------------------------------------------------------------------
# TRAINING STEP
# ---------------------------------------------------------------------
estimator = SKLearn(
    entry_point="train_model.py",
    source_dir="src/training",
    framework_version="1.2-1",
    role=role,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    hyperparameters={
        "n_estimators": n_estimators,
        "max_depth": 10,
        "random_state": 58,
        "train_file_name": "train.csv",
        "target": "fare_amount",
    },
    sagemaker_session=session,
    output_path=Join(on="/", values=["s3:/", unified_bucket, "models"]),
)

training_step = TrainingStep(
    name="NYCTaxiTraining",
    estimator=estimator,
    inputs={
        "train": TrainingInput(
            s3_data=processing_step.properties
            .ProcessingOutputConfig.Outputs["processed"]
            .S3Output.S3Uri
        )
    },
)

# ---------------------------------------------------------------------
# EVALUATION STEP
# ---------------------------------------------------------------------
evaluation_report = PropertyFile(
    name="EvaluationReport",
    output_name="evaluation",
    path="evaluation.json",
)

eval_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_type="ml.t3.xlarge",
    instance_count=1,
    base_job_name="nyc-taxi-evaluation",
    sagemaker_session=session,
)

evaluation_step = ProcessingStep(
    name="NYCTaxiEvaluation",
    processor=eval_processor,
    inputs=[
        ProcessingInput(
            source=training_step.properties.ModelArtifacts.S3ModelArtifacts,
            destination="/opt/ml/processing/model",
        ),
        ProcessingInput(
            source=processing_step.properties
            .ProcessingOutputConfig.Outputs["processed"]
            .S3Output.S3Uri,
            destination="/opt/ml/processing/input",
            input_name="data",
        ),
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/evaluation",
            output_name="evaluation",
            destination=Join(on="/", values=["s3:/", unified_bucket, "evaluation"]),
        )
    ],
    code="src/evaluation/evaluate.py",
    job_arguments=[
        "--model_name", "model.pkl",
        "--model_dir", "/opt/ml/processing/model",
        "--data_dir", "/opt/ml/processing/input",
        "--train_file_name", "train.csv",
        "--test_file_name", "test.csv",
        "--target", "fare_amount",
    ],
    property_files=[evaluation_report],
)

# ---------------------------------------------------------------------
# PIPELINE FACTORY (CRITICAL)
# ---------------------------------------------------------------------
def build_pipeline(register_model: bool) -> Pipeline:
    steps = [
        processing_step,
        training_step,
        evaluation_step,
    ]

    if register_model:
        rmse_condition = ConditionLessThanOrEqualTo(
            left=JsonGet(
                step_name=evaluation_step.name,
                property_file=evaluation_report,
                json_path="$.test_score.RMSE",
            ),
            right=3000,
        )

        model_metrics = ModelMetrics(
            model_statistics=MetricsSource(
                s3_uri=Join(
                    on="/",
                    values=[
                        evaluation_step.properties
                        .ProcessingOutputConfig.Outputs["evaluation"]
                        .S3Output.S3Uri,
                        "evaluation.json",
                    ],
                ),
                content_type="application/json",
            )
        )

        model = SKLearnModel(
            model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
            role=role,
            entry_point="train_model.py",
            source_dir="src/training",
            framework_version="1.2-1",
            sagemaker_session=session,
        )

        register_step_args = model.register(
            content_types=["text/csv"],
            response_types=["text/csv"],
            inference_instances=["ml.m5.large"],
            transform_instances=["ml.m5.xlarge"],
            model_package_group_name="NYCTaxiFareModels",
            model_metrics=model_metrics,
            approval_status="PendingManualApproval",
            description="NYC Taxi Fare Prediction model",
        )

        register_step = ModelStep(
            name="RegisterModel",
            step_args=register_step_args,
        )

        condition_step = ConditionStep(
            name="RMSECheck",
            conditions=[rmse_condition],
            if_steps=[register_step],
            else_steps=[],
        )

        steps.append(condition_step)

    return Pipeline(
        name="NYCTaxiFarePredictionPipeline",
        parameters=[unified_bucket, n_estimators],
        steps=steps,
        sagemaker_session=session,
    )

#updates 1 2 3 4 5 6 7

# ---------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------
if __name__ == "__main__":
    args = parse_args()

    if args.validate_only:
        build_pipeline(register_model=False)
        print("✅ CI validation successful (no AWS mutations)")

    elif args.execute:
        pipeline = build_pipeline(register_model=True)
        pipeline.upsert(role_arn=role)
        print("🚀 Pipeline upserted successfully")

    else:
        raise RuntimeError(
            "You must specify --validate-only (CI) or --execute (CD/orchestration)"
        )
