import json
import os
import argparse
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, root_mean_squared_error, r2_score, mean_absolute_error
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def parse_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--data_dir", required=True)
    parser.add_argument("--train_file_name", required=True)
    parser.add_argument("--val_file_name", required=True)
    parser.add_argument("--model_dir", required=True)
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--output_dir", required=True)
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
    
    preds = model.predict(X_val)
    val_scores = {
    "MAE":mean_absolute_error(y_val, preds),
    "MSE":mean_squared_error(y_val, preds),
    "RMSE":root_mean_squared_error(y_val, preds),
    "R2":r2_score(y_val, preds)
    }
    
    return val_scores

def main():
    score = {}

    model_path = os.path.join(args.model_dir, args.model_name)
    model = joblib.load(model_path)
    
    train_file = os.path.join(args.data_dir, args.train_file_name)
    train_df = pd.read_csv(train_file)

    test_file = os.path.join(args.data_dir, args.val_file_nam)
    test_df = pd.read_csv(test_file)

    score["train_score"] = evaluate(model, train_df)
    score["test_score"] = evaluate(model, test_df)

    os.makedirs(args.output_dir, exist_ok=True)
    path_output = os.path.join(args.output_dir, args.output_name)
    
    with open(path_output, "w") as f:
        json.dump(score, f)

if __name__ == "__main__":
    main()