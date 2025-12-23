import boto3
import pandas as pd 
import argparse
import io
import logging
from botocore.exceptions import ClientError
from sklearn.model_selection import train_test_split
import os
from boto3.s3.transfer import TransferConfig

logging.basicConfig(level=logging.INFO,    
					format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
					datefmt='%Y-%m-%d %H:%M:%S')

args = None

def parse_arguments():
	global args
	parser = argparse.ArgumentParser()
	parser.add_argument("--input_file_path", required=False, default="/opt/ml/processing/input/yellow_tripdata_v1.parquet")
	parser.add_argument("--is_local", required=False)
	parser.add_argument("--format", default="parquet")
	parser.add_argument("--output_train_file_path", required=False, default="/opt/ml/processing/output/train.csv")
	parser.add_argument("--output_test_file_path", required=False, default="/opt/ml/processing/output/test.csv")
	parser.add_argument("--target", required=True)

	args = parser.parse_args()

parse_arguments()

client = boto3.client("s3")
def load_data():
	input_path = args.input_file_path

	logging.info(f"loading data from {input_path}")

	if args.format == "parquet":
		data = pd.read_parquet(input_path)
		logging.info(f"Data is loaded succesfully")
		return data
	else: 
		data = pd.read_csv(input_path)
		logging.info(f"Data is loaded succesfully")
		return data

def process_data(data: pd.DataFrame):
	columns_to_remove = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "RatecodeID", "store_and_fwd_flag",
                     "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee" ,"cbd_congestion_fee"]
	
	data_raw_selected = data.drop(columns=columns_to_remove, axis=1)
	logging.info(f"{columns_to_remove} are dropped")
	
	logging.info(f"{data_raw_selected.isna().sum()} are dropped")

	logging.info(f"Null values \n{data_raw_selected.isna().sum()}")
	data_raw_selected.passenger_count.fillna(data_raw_selected.passenger_count.mode()[0], inplace=True)
	
	logging.info(f"Total duplicates \n{data_raw_selected.duplicated().sum()}")
	data_raw_selected.drop_duplicates(inplace=True)
	return data_raw_selected


def save_data(data: pd.DataFrame, save_path):
    try: 
        data.to_csv(save_path)
        logging.info(f"Processed data uploaded to {save_path}")

    except Exception as e:
        logging.error(f"Error while uploading processed data: {e}", exc_info=True)


def process_and_save_data():
	try:
		data = load_data()
		processed_data = process_data(data)

		inde = processed_data.drop(args.target, axis=1)
		target = processed_data[args.target]

		X_train, x_test, y_train, y_test = train_test_split(inde,target, test_size=0.2, shuffle=True)
		train_data = pd.concat([X_train, y_train], axis=1)
		test_data = pd.concat([x_test, y_test], axis=1)

		save_data(train_data, args.output_train_file_path)
		save_data(test_data, args.output_test_file_path)
		

		logging.info(f'Successfully Done processing and saving data.')
	except Exception as e:
		logging.error(f'Error while processing data {e}')

if __name__ == "__main__":
	process_and_save_data()
