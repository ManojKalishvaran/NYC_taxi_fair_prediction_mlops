import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import TrainingStep, ProcessingStep
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.workflow.parameters import (ParameterString, ParameterInteger)

session = sagemaker.Session()
role = "arn:aws:iam::818831377059:role/sagemaker_execusion_role"

raw_bucket = ParameterString(
	name="RawDataBucket",
	default_value="nyc-taxi-mlops-raw-data"
)

processed_bucket = ParameterString(
	name="ProcessedBucket",
	default_value="nyc-taxi-processed-818831377059"
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
        "train": f"s3://{processed_bucket}/processed/"
    }
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
        training_step
    ],
    sagemaker_session=session
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    execution = pipeline.start()
    print(f"Pipeline started: {execution.arn}")