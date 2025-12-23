import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import TrainingStep, ProcessingStep
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.workflow.parameters import (ParameterString, ParameterInteger)
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.functions import Join
from pipeline_config import RAW_BUCKT, RAW_FOLDER, PROCESSED_BUCKET, PROCESSED_FOLDER, ROLE


session = sagemaker.Session()
role = ROLE

raw_bucket = ParameterString(
	name="RawDataBucket",
	default_value=RAW_BUCKT
)

processed_bucket = ParameterString(
	name="ProcessedBucket",
	default_value=PROCESSED_BUCKET
)

model_bucket = ParameterString(
	name="ModelBucket",
	default_value="amazon-sagemaker-818831377059-us-east-1-be63etunzlvelj"
)

n_estimators = ParameterInteger(
	name="n_estimators",
	default_value=200
)

# Processing
processor = SKLearnProcessor(
	framework_version="1.2-1",
	role=role,
	instance_type="ml.t3.xlarge",
	instance_count=1,
	base_job_name="nyc-taxi-preprocess",
	sagemaker_session=session
)

processing_step = ProcessingStep(
	name="NYCTaxiPreprocessing",
	processor=processor,
	code="src/preprocessing/load_data.py",
	job_arguments=[
		"--bucket", raw_bucket,
        "--key", "v1/yellow_tripdata_v1.parquet",
        "--format", "parquet",
        "--save_bucket", processed_bucket,
        "--save_folder", "processed",
        "--save_name", "train",
        "--test_save_name", "test",
        "--save_format", "csv",
        "--target", "fare_amount"
	]
)

# Training 
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
        "target": "fare_amount"
    },
    sagemaker_session=session
)

training_step = TrainingStep(
    name="NYCTaxiTraining",
    estimator=estimator,
    inputs={
        "train": Join(
            on="/",
            values=[processed_bucket, "processed"]
        )
    }
)


# evaluation 
evaluation_report = PropertyFile(
    name="EvaluationReport",
    output_name="evaluation",
    path="evaluation.json"
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
            destination="/opt/ml/processing/model"
        ),
        ProcessingInput(
            source=Join(
                on="/",
                values=[processed_bucket, "processed"]
            ),
            destination="/opt/ml/processing/test"
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/evaluation",
            output_name="evaluation"
        )
    ],
    code="src/evaluation/evaluate.py",
    property_files=[evaluation_report]
)


# condition
rmse_condition = ConditionLessThanOrEqualTo(
    left=JsonGet(
        step_name=evaluation_step.name,
        property_file=evaluation_report,
        json_path="test_score.RMSE"
    ),
    right=4.5
)

condition_step = ConditionStep(
    name="RMSECheck",
    conditions=[rmse_condition],
    if_steps=[],
    else_steps=[]
)



# Pipeline definition

pipeline = Pipeline(
    name="NYCTaxiFarePredictionPipeline",
    parameters=[
        raw_bucket,
        processed_bucket,
        model_bucket,
        n_estimators
    ],
    steps=[
        processing_step,
        training_step,
        evaluation_step,
        condition_step
    ],
    sagemaker_session=session
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    execution = pipeline.start()
    print(f"Pipeline started: {execution.arn}")