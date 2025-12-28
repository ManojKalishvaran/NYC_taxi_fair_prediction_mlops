import os
import argparse
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def parse_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--n_estimators", type=int, default=200)
    parser.add_argument("--max_depth", type=int, default=10)
    parser.add_argument("--random_state", type=int, default=58)
    parser.add_argument("--train_file_name", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--model_save_name", default="model.pkl")
    parser.add_argument("--is_local", type=bool, default=False)
    
    return parser.parse_args()

logging.info("Parsing args")
args = parse_args()

if args.is_local:
    from dotenv import load_dotenv 
    logging.info("Adding auth keys")
    load_dotenv()
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("Access_key_id")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("Secret_access_key")

def load_data():
    logging.info("Loading data...")
    training_path = os.environ["SM_CHANNEL_TRAIN"]
    train_df = pd.read_csv(
        os.path.join(training_path, args.train_file_name)
    )
    return train_df

 
def train_model(train_df:pd.DataFrame):
    logging.info("Splitting for inde & target data...")
    X=train_df.drop(args.target, axis=1)
    y = train_df[args.target]
    
    model = RandomForestRegressor(
    n_estimators=args.n_estimators,
    max_depth=args.max_depth,
    random_state=args.random_state,
    n_jobs=-1
    )

    logging.info("Training...")
    model.fit(X, y)
    return model

 
def save_models(model: RandomForestRegressor):
    logging.info("Saving models...")
    model_dir = os.environ["SM_MODEL_DIR"]
    # if args.is_local:
    #     model_dir = 
    logging.info(f"Model save path {os.path.join(model_dir, args.model_save_name)}")
    joblib.dump(model, os.path.join(model_dir, args.model_save_name))
    logging.info("Model saving is done!")

def main():
    train_df = load_data()
    model = train_model(train_df)        
    logging.info(f"Training is done!")
    save_models(model)
 
if __name__ == "__main__":
    main()