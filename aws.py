import os
import pandas as pd
from config import DB_TABLELIST_DICT


import boto3

sess = boto3.Session(region_name='ap-northeast-2')
s3client = sess.client("s3")
bucket_name = 'haneulkim-s3-tmdb'

# Create bucket
def create_bucket(bucket_name):
    s3_location_constr = {'LocationConstraint': 'ap-northeast-2'}
    s3client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=s3_location_constr)
# create_bucket()


# Loading local data to S3
cwd_path = os.getcwd()
file_path = "data/tmdb_ratins.parquet"
output_path = 'tmdb/ratings.parquet'

s3client.upload_file(
    os.path.join(cwd_path, "data/tmdb_ratins.parquet"),
    bucket_name,
    output_path
)


# Create S3 metadata table for athena query.