from plugins.s3_csv import open_s3
from plugins.s3_csv import upload_to_s3
from datetime import datetime

def upload_311_s3(ti):
    aws_access_key = 'AKIAZN3AE5KWHHQAHJHV'
    aws_access_secret_key = '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'
    bucket = 'jirving-sf311' #bucket name
    int_311_results = ti.xcom_pull(task_ids="sf311_preprocess_2") #xcom_pull is used to pull data from another task
    #Upload results to S3 bucket with naming convention of current datetime + raw_311_data
    int_311_key = f'int_311_data/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv' #export 
    s3_client = open_s3(aws_access_key, aws_access_secret_key)
    upload_to_s3(s3_client, int_311_results, bucket, int_311_key)