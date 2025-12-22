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
	parser.add_argument("--bucket", required=True)
	parser.add_argument("--is_local", required=False)
	parser.add_argument("--key", required=True)
	parser.add_argument("--format", default="parquet")
	parser.add_argument("--save_bucket", required=True)
	parser.add_argument("--save_folder", required=True)
	parser.add_argument("--save_format", default="csv")
	parser.add_argument("--save_name", required=True)
	parser.add_argument("--target", required=True)
	parser.add_argument("--test_save_name", required=True)

	args = parser.parse_args()

parse_arguments()

save_bucket = args.save_bucket
save_folder = args.save_folder
save_format = args.save_format
save_name_train = args.save_name
save_name_test = args.test_save_name
BUCKET = args.bucket
KEY = args.key

if args.is_local == "true":
	from dotenv import load_dotenv
	load_dotenv()
	os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("Access_key_id")
	os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("Secret_access_key")

client = boto3.client("s3")
def load_data():
	logging.info(f"loading {KEY} from {BUCKET}")

	config = TransferConfig(
		multipart_threshold= 10*1024*1024,
		max_concurrency= 8,
		multipart_chunksize=8*1024*1024,
		use_threads=True
	)

	buffer = io.BytesIO()

	client.download_fileobj(
		Bucket=BUCKET,
		Key=KEY,
		Fileobj=buffer,
		Config=config
	)
	buffer.seek(0)
	
	if args.format == "parquet":
		data = pd.read_parquet(buffer)
		logging.info(f"Data is loaded succesfully")
		return data
	else: 
		data = pd.read_csv(buffer)
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

# def save_data_s3(data: pd.DataFrame, save_bucket, key):
# 	try:
# 		buffer = io.BytesIO()
# 		data.to_csv(buffer, index=False, encoding="utf-8")
# 		data_str = buffer.read()
# 		logging.info(f"data string type : {type(data_str) = }")
# 		client.put_object(
# 			Bucket = save_bucket,
# 			Key = key,
# 			Body = data_str
# 		)
# 		logging.info(f"Processed data uploaded to {save_bucket}- {key}")
# 	except Exception as e:
# 		logging.error(f"Error while uploading processed data: {e}")

def save_data_s3(data: pd.DataFrame, save_bucket, key):
    try:

        data_bytes = data.to_csv(index=False).encode("utf-8")

        logging.info(f"data_bytes type: {type(data_bytes)}")

        client.put_object(
            Bucket=save_bucket,
            Key=key,
            Body=data_bytes
        )

        logging.info(f"Processed data uploaded to {save_bucket}/{key}")

    except Exception as e:
        logging.error(f"Error while uploading processed data: {e}", exc_info=True)


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

		inde = processed_data.drop(args.target, axis=1)
		target = processed_data[args.target]

		X_train, x_test, y_train, y_test = train_test_split(inde,target, test_size=0.2, shuffle=True)
		train_data = pd.concat([X_train, y_train], axis=1)
		test_data = pd.concat([x_test, y_test], axis=1)

		# ensure_save_bucket()
		train_save_key = save_folder+"/"+save_name_train+"."+save_format
		test_save_key = save_folder+"/"+save_name_test+"."+save_format
		save_data_s3(train_data, save_bucket, train_save_key)
		save_data_s3(test_data, save_bucket, test_save_key)

		logging.info(f'Successfully Done processing and saving data.')
	except Exception as e:
		logging.error(f'Error while processing data {e}')

if __name__ == "__main__":
	process_and_save_data()
