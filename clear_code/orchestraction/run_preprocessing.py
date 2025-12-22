from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker import get_execution_role

# ROLE = get_execution_role()
ROLE = "arn:aws:iam::818831377059:role/sagemaker_execusion_role"

processor = SKLearnProcessor(
	framework_version="1.2-1",
	role=ROLE,
	instance_type="ml.t3.xlarge",
	instance_count=1,
	base_job_name="nyc-taxi-preprocess",
)

processor.run(
	code="src/preprocessing/load_data.py",
	arguments=[
		"--bucket", "nyc-taxi-mlops-raw-data",
        "--key", "v1/yellow_tripdata_v1.parquet",
        "--format", "parquet",
        "--save_bucket", "nyc-taxi-processed-bucket",
        "--save_folder", "processed",
        "--save_name", "train",
        "--test_save_name", "test",
        "--save_format", "csv",
        "--target", "fare_amount"
	],
)