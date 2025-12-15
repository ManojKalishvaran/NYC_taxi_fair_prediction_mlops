import boto3
import pandas as pd 
import argparse
import io
import logging

logging.basicConfig(level=logging.INFO,    
					format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
					datefmt='%Y-%m-%d %H:%M:%S')

args = None

def parse_arguments():
	global args
	parser = argparse.ArgumentParser()
	parser.add_argument("--bucket", required=True)
	parser.add_argument("--key", required=True)
	parser.add_argument("--format", default="parquet")

	args = parser.parse_args()

parse_arguments()

BUCKET = args.bucket
KEY = args.key

client = boto3.client("s3")
def load_data():
	logging.info(f"loading {KEY} from {BUCKET}")
	res = client.get_object(Bucket=BUCKET, Key=KEY)	
	data_by = res["Body"].read()
	if args.format == "parquet":
		data = pd.read_parquet(io.BytesIO(data_by))
		logging.info(f"Data is loaded succesfully")
		return data
	else: 
		data = pd.read_csv(io.BytesIO(data_by))
		logging.info(f"Data is loaded succesfully")
		return data

def process_data(data: pd.DataFrame):
	columns_to_remove = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "RatecodeID", "store_and_fwd_flag",
                     "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee" ,"cbd_congestion_fee"]
	
	data_raw_selected = data.drop(columns=columns_to_remove, axis=1)
	logging.info(f"{columns_to_remove} are dropped")
	
	logging.info(f"Null values \n{data_raw_selected.isna().sum()}")
	data_raw_selected.passenger_count.fillna(data_raw_selected.passenger_count.mode()[0], inplace=True)
	
	data_raw_selected.drop_duplicates(inplace=True)

if __name__ == "__main__":
	data = load_data()
	process_data(data)
