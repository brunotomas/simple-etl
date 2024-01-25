from datetime import datetime
from utils import s3_connect

def upload_parquet_to_s3(parquet_file_path, bucket_name):

    s3 = s3_connect()
    now = datetime.now()
    object_key = f"{int(now.timestamp())}.parquet"

    # Upload the Parquet file to the specified S3 bucket and object key
    try:
        s3.upload_file(parquet_file_path, bucket_name, object_key)
        print(f"Parquet file '{parquet_file_path}' uploaded to S3 bucket '{bucket_name}' with object key '{object_key}'")
        return(f"s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"Error uploading Parquet file to S3: {e}")

if __name__ == "__main__":

    bucket_name = 'books-landing'
    parquet_file_path = 'output_data_1706210538.parquet'

    # Upload the Parquet file to the specified S3 bucket and object key
    upload_parquet_to_s3(parquet_file_path, bucket_name)