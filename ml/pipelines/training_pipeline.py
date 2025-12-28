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
from sagemaker.inputs import TrainingInput
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.functions import Join
from sagemaker.workflow.model_step import ModelStep
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.sklearn.model import SKLearnModel


session = PipelineSession()


from pipeline_config import RAW_BUCKT, RAW_FOLDER, PROCESSED_BUCKET, PROCESSED_FOLDER, ROLE


# session = sagemaker.Session()
role = ROLE


unified_bucket = ParameterString(
    name="UnifiedBucket",
    default_value=RAW_BUCKT
)

endpoint_name = ParameterString(
    name="EndpointName",
    default_value="nyc-taxi-fare-endpoint"
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
inputs=[
    ProcessingInput(
        source=Join(
            on="/",
            values=[
                "s3:/",
                unified_bucket,
                "data/raw/v1"
            ]
        ),
        destination="/opt/ml/processing/input"
    )
],

outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/output",
            output_name="processed",
            destination=Join(on="/", values=["s3:/", unified_bucket, "data/processed/v1"])
        )
    ],

code="src/preprocessing/load_data.py",
job_arguments=[
    "--input_file_path", "/opt/ml/processing/input/yellow_tripdata_v1.parquet",
    "--output_train_file_path", "/opt/ml/processing/output/train.csv",
    "--output_test_file_path", "/opt/ml/processing/output/test.csv",
    "--target", "fare_amount",
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
        "train_file_name": "train.csv",   # ADD
        "target": "fare_amount",          # ADD

    },
    sagemaker_session=session,
    output_path=Join(
    on="/",
    values=[
        "s3:/",
        unified_bucket,
        "models"
    ]
)
)

training_step = TrainingStep(
    name="NYCTaxiTraining",
    estimator=estimator,
    inputs={
        "train": TrainingInput(
            s3_data=processing_step.properties.ProcessingOutputConfig.Outputs["processed"].S3Output.S3Uri
        )
    },
    
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
            source=processing_step.properties.ProcessingOutputConfig.Outputs["processed"].S3Output.S3Uri,
            destination="/opt/ml/processing/input",
            input_name="data"
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/evaluation",
            output_name="evaluation",
            destination=Join(on="/", values=["s3:/", unified_bucket, "evaluation"])
        )
    ],
    code="src/evaluation/evaluate.py",
    job_arguments=[
        "--model_name", "model.pkl",
        "--model_dir", "/opt/ml/processing/model",
        "--data_dir", "/opt/ml/processing/input",  # Explicitly set
        "--train_file_name", "train.csv",
        "--test_file_name", "test.csv",
        "--target", "fare_amount"
    ],
    property_files=[evaluation_report],
)


from sagemaker.processing import ScriptProcessor
from sagemaker.workflow.steps import ProcessingStep

notify_processor = ScriptProcessor(
    image_uri="683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3",
    role=role,
    instance_type="ml.t3.xlarge",
    instance_count=1,
    command=["python3"],
    sagemaker_session=session,
)


rmse_condition = ConditionLessThanOrEqualTo(
    left=JsonGet(
        step_name=evaluation_step.name,
        property_file=evaluation_report,
        json_path="$.test_score.RMSE"
    ),
    right=3000
)


model_metrics = ModelMetrics(
    model_statistics=MetricsSource(
        s3_uri=Join(
            on="/",
            values=[
                evaluation_step.properties.ProcessingOutputConfig.Outputs["evaluation"].S3Output.S3Uri,
                "evaluation.json",
            ]
        ),
        content_type="application/json"
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

register_model_step = ModelStep(
    name="NYCTaxiRegisterModel",
    step_args=register_step_args
)


condition_step = ConditionStep(
    name="RMSECheck",
    conditions=[rmse_condition],
    if_steps=[register_model_step],
    else_steps=[]
)

# Pipeline definition
pipeline = Pipeline(
    name="NYCTaxiFarePredictionPipeline",
    parameters=[
        unified_bucket,
        n_estimators,
    ],
    steps=[
        processing_step,
        training_step,
        evaluation_step,
        condition_step
    ],
    sagemaker_session=session
)
#new line updated 2 3 4 5 6

if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    # execution = pipeline.start()
    # print(f"Pipeline started: {execution.arn}")