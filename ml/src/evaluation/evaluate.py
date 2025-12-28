import json
import os
import argparse
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def parse_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        "--data_dir",
        default="/opt/ml/processing/test"
    )

    parser.add_argument(
        "--model_dir",
        default="/opt/ml/processing/model"
    )

    parser.add_argument(
        "--output_dir",
        default="/opt/ml/processing/evaluation"
    )

    parser.add_argument("--train_file_name", required=True)
    parser.add_argument("--test_file_name", required=True)
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--output_name", default="evaluation.json")
    parser.add_argument("--target", default="fare_amount")
    parser.add_argument("--is_local", type=bool, default=False)
    
    return parser.parse_args()

logging.info("Parsing args")
args = parse_args()

def evaluate(model: RandomForestRegressor, val_df:pd.DataFrame):
    logging.info("Validating...")
    X_val = val_df.drop(args.target, axis=1)
    y_val = val_df[args.target]
    logging.info("split data....")
    preds = model.predict(X_val)
    logging.info("making prediction data....")
    val_scores = {
    "MAE":mean_absolute_error(y_val, preds),
    "MSE":mean_squared_error(y_val, preds),
    "RMSE":mean_squared_error(y_val, preds, squared=False),
    "R2":r2_score(y_val, preds)
    }
    logging.info(f"Done evaluation :\n{val_scores}....")
    
    return val_scores

def main():
    score = {}

    # model_path = os.path.join(args.model_dir, args.model_name)
    # model = joblib.load(model_path)
        # Extract model.tar.gz (SageMaker TrainingJob format)
    model_tar_path = os.path.join(args.model_dir, 'model.tar.gz')
    model_path = os.path.join(args.model_dir, args.model_name)
    
    if os.path.exists(model_tar_path):
        import tarfile
        with tarfile.open(model_tar_path, 'r:gz') as tar:
            tar.extractall(args.model_dir)
        logging.info(f"Extracted model.tar.gz to {args.model_dir}")
    
    logging.info("Model is being loaded...")
    # Load model.pkl (now exists after extraction)
    model = joblib.load(model_path)
    logging.info("Model is loaded!")

    train_file = os.path.join(args.data_dir, args.train_file_name)
    train_df = pd.read_csv(train_file)

    test_file = os.path.join(args.data_dir, args.test_file_name)
    test_df = pd.read_csv(test_file)

    logging.info(f"Evaluating training data...")
    score["train_score"] = evaluate(model, train_df)

    logging.info(f"Evaluating testing data...")
    score["test_score"] = evaluate(model, test_df)

    os.makedirs(args.output_dir, exist_ok=True)
    path_output = os.path.join(args.output_dir, args.output_name)
    
    logging.info(f"Writting output to json...")
    with open(path_output, "w") as f:
        json.dump(score, f)
    logging.info(f"Output is written to json...")

if __name__ == "__main__":
    main()