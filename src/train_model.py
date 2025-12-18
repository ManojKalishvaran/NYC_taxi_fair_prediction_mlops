import os
import argparse
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, root_mean_squared_error, r2_score, mean_absolute_error
from dotenv import load_dotenv 

def parse_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--n_estimators", type=int, default=200)
    parser.add_argument("--max_depth", type=int, default=10)
    parser.add_argument("--random_state", type=int, default=58)
    parser.add_argument("--train_file_name", required=True)
    parser.add_argument("--val_file_name", required=False)
    parser.add_argument("--target", required=True)
    parser.add_argument("--model_save_name", default="model.pkl")
    parser.add_argument("--is_local", type=bool, default=False)
    
    return parser.parse_args()
 
args = parse_args()
 
def load_data():
    if not args.is_local:
        training_path = os.environ.get("SM_CHANNEL_TRAIN")
        val_path = os.environ.get("SM_CHANNEL_VALIDARION")
        
        train_df = pd.read_csv(os.path.join(training_path, args.train_file_name))
        if val_path:
            val_df = pd.read_csv(os.path.join(val_path, args.val_file_name))
        else:
            val_df = None
    else:
        load_dotenv()
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("")
        os.environ["AWS_SECRET"] = os.getenv("")

    return train_df, val_df
 
def train_model(train_df:pd.DataFrame):
    X=train_df.drop(args.target, axis=1)
    y = train_df[args.target]
    
    model = RandomForestRegressor(
    n_estimators=args.n_estimators,
    max_depth=args.max_depth,
    random_state=args.random_state,
    n_jobs=-1
    )
    
    model.fit(X, y)
    return model
 
def evaluate(model: RandomForestRegressor, val_df:pd.DataFrame):
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
 
def save_models(model: RandomForestRegressor):
    model_dir = os.environ["SM_MODEL_DIR"]
    os.makedirs(model_dir, exist_ok=True)
    joblib.dump(model, os.path.join(model_dir, args.model_save_name))
 
def main():
    train_df, val_df = load_data()
    model = train_model(train_df)
    if val_df is not None:
        evaluate(model, val_df)
        
    save_models(model)
 
if __name__ == "__main__":
    main()