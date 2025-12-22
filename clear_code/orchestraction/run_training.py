from sagemaker.sklearn.estimator import SKLearn
from sagemaker import get_execution_role

# ROLE = get_execution_role()
ROLE="arn:aws:iam::818831377059:role/sagemaker_execusion_role"

estimator = SKLearn(
    entry_point="train_model.py",
    source_dir="src",
    framework_version="1.2-1",
    role=ROLE,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    hyperparameters={
        "n_estimators": 200,
        "max_depth": 10,
        "random_state": 58,
        "train_file_name": "train.csv",
        "target": "fare_amount"
    },
)

estimator.fit({
    "train": "s3://nyc-taxi-processed-bucket/processed/"
})
