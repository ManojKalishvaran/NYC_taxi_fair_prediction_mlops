import boto3
import pandas as pd 
import argparse
import io
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("Access_key_id")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("Secret_access_key")

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
	parser.add_argument("--save_bucket", required=True)
	parser.add_argument("--save_folder", required=True)
	parser.add_argument("--save_format", default="csv")
	parser.add_argument("--save_name", required=True)

	args = parser.parse_args()

parse_arguments()

save_bucket = args.save_bucket
save_folder = args.save_folder
save_format = args.save_format
save_name = args.save_name
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
	
	logging.info(f"{data_raw_selected.isna().sum()} are dropped")

	logging.info(f"Null values \n{data_raw_selected.isna().sum()}")
	data_raw_selected.passenger_count.fillna(data_raw_selected.passenger_count.mode()[0], inplace=True)
	
	logging.info(f"Total duplicates \n{data_raw_selected.duplicated().sum()}")
	data_raw_selected.drop_duplicates(inplace=True)
	return data_raw_selected

def save_data_s3(data: pd.DataFrame):
	try:
		buffer = io.BytesIO()
		data.to_csv(buffer, index=False)
		data_str = buffer.getvalue()
		key = save_folder+"/"+save_name+"."+save_format
		client.put_object(
			Bucket = save_bucket,
			Key = key,
			Body = data_str
		)
		logging.info(f"Processed data uploaded to {save_bucket}- {save_folder}-{save_name}.{save_format}")
	except Exception as e:
		logging.error(f"Error while uploading processed data: {e}")

def ensure_save_bucket():
	try:
		client.head_bucket(Bucket=save_bucket)
		logging.info(f'Save bucket {save_bucket} exist.')
	except ClientError:
		logging.info(f'Save bucket {save_bucket} not found, creating one.')
		client.create_bucket(
			Bucket = save_bucket,
			# CreateBucketConfiguration={"LocationConstrain"}
		)

def process_and_save_data():
	try:
		data = load_data()
		processed_data = process_data(data)
		ensure_save_bucket()
		save_data_s3(processed_data)
		logging.info(f'Successfully Done processing and saving data.')
	except Exception as e:
		logging.error(f'Error while processing data {e}')

if __name__ == "__main__":
	process_and_save_data()
