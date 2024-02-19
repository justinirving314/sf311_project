import os
import sys
# Get the current working directory
cwd = os.getcwd()
# Navigate up one level in the directory hierarchy
parent_dir = os.path.abspath(os.path.join(cwd, '..'))
relative_path = os.path.join(parent_dir+'/plugins/')
# Add plugins folder for access to functions
sys.path.append(relative_path)


#Credentials loaded from .env file, new users of this notebook must create their own credentials and .env file
#after cloning the gitrepo
from dotenv import load_dotenv
load_dotenv()
app_token = os.getenv('APP_TOKEN')
api_key_id = os.getenv('API_KEY_ID')
api_secret_key = os.getenv('API_SECRET_KEY')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')
crawler_name = 'sfdata_311_s3_glue_crawler'

# Define some additional variables needed for the pipeline run.
bucket = 'jirving-sf311' #os.getenv('S3_BUCKET')
bucket_key = 'int_311_data/' #bucket key used for data uploads and reads, where raw csv data from SF 311 is stored
site = 'data.sfgov.org'
endpoint = 'vw6y-z8j6' # 311 data endpoint
geo_endpoint = 'sevw-6tgi' # neighborhoods with census tract geo endpoint
df_spatial_txt = 'the_geom' # field name used in geo_endpoint for spatial joins
region = 'us-west-1' # AWS region but this is not used as S3Hook is being used now (can delete)
aws_conn_id_local = 'aws_default' #this is the name of the AWS connection configured in your airflow instance that is called in S3 hooks

#Import other libraries used in main DAG code
import json
import datetime
from datetime import datetime, timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import BaseOperator
from airflow.models import TaskInstance
from sf311_extract import pull_opensf_data
from sf311_preprocess_1 import raw_311_preprocess
from sf311_preprocess_2 import address_to_coordinates
from sf311_preprocess_2 import add_tracts
from sf311_s3_upload import upload_to_s3_new
from airflow.hooks.S3_hook import S3Hook
from sf311_extract_date import read_all_bucket_hook
import pickle
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import base64
import boto3
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook




SOURCE_FILE_PATH = '/opt/airflow/dags/files'


# Define function for uploading final dataframe to S3. This can be moved to plugins
def upload_to_s3_hook(df, bucket):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id_local)  # Assumes you have configured an AWS connection in Airflow
    # int_311_key = f'{bucket_key}{datetime.now():%Y-%m-%d_%H-%M-%S}.csv' #export 
    int_311_key = f'{bucket_key}{datetime.now():%Y-%m-%d_%H-%M-%S}.parquet'
    parquet_buffer = io.BytesIO()
    df_pa = pa.Table.from_pandas(df)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(df_pa, parquet_buffer)
    # csv_string = df.to_csv(index=False)
    s3_hook.load_bytes(bytes_data=parquet_buffer.getvalue().to_pybytes(), key=int_311_key, bucket_name=bucket)
    


def trigger_glue_crawler(aws_local_conn_id, crawler_name, region):
    # Initialize GlueCrawlerHook
    glue_hook = GlueCrawlerHook(aws_conn_id=aws_local_conn_id, region_name = region)

    # Trigger the Glue crawler
    response = glue_hook.start_crawler(crawler_name)



# Define DAG instance with name and default run
sf311_upload_dag = DAG(
    dag_id='sf_data_upload',
    schedule_interval=timedelta(days=1),
    start_date=airflow.utils.dates.days_ago(1),
    tags=["data_pipeline"]
)

# Define task to check the latest date in the S3 bucket and save that value for use in the next task
s3_extract_last_task = PythonOperator(
    task_id='s3_extract_last_task',
    python_callable=read_all_bucket_hook,
    op_kwargs= {'access_key': aws_access_key, 
                     'access_secret_key' : aws_access_secret_key, 
                     'region' : region,
                     'bucket' : bucket,
                     'key' : bucket_key},
    dag=sf311_upload_dag,
)

#old filter to be restored: (f"requested_datetime >= '{s3_extract_last_task.output}'")
# Define task to extract data from the SF311 data source
sf311_extract_task = PythonOperator(
    task_id='sf311_extract_task',
    python_callable=pull_opensf_data,
    op_kwargs= {'site': site, 
                     'endpoint' : endpoint, 
                     'app_token' : app_token,
                     'api_key_id' : api_key_id,
                     'api_secret_key' : api_secret_key,
                     'pulltype':'all',
                     'filters': (f"requested_datetime >= '{s3_extract_last_task.output}'")},
    dag=sf311_upload_dag,
)

# Define task to clean extracted data
sf311_clean_task = PythonOperator(
    task_id='sf311_clean_task',
    python_callable=raw_311_preprocess,
    op_kwargs={'df' : sf311_extract_task.output},
    dag=sf311_upload_dag,
)

# Define task to add coordinates to address that have a street address but are missing coordinates for no apparent reason
sf311_add_coord_task = PythonOperator(
    task_id='sf311_add_coord_task',
    python_callable=address_to_coordinates,
    op_kwargs={'df' : sf311_clean_task.output},
    dag=sf311_upload_dag,
)

# Define task to spatially join complaints with neighborhoods/tracts
sf311_add_neighborhoods_task = PythonOperator(
    task_id='sf311_add_neighborhoods_task',
    python_callable=add_tracts,
    op_kwargs={'df' : sf311_add_coord_task.output,
               'df_spatial_txt': df_spatial_txt,
               'site': site, 
                'geo_endpoint' : geo_endpoint, 
                'app_token' : app_token,
                'api_key_id' : api_key_id,
                'api_secret_key' : api_secret_key},
    dag=sf311_upload_dag,
)

# Define task to upload final dataframe of results to S3 bucket
sf311_s3_upload_task = PythonOperator(
    task_id='sf311_s3_upload_task',
    python_callable=upload_to_s3_hook,
    op_kwargs={'df' : sf311_add_neighborhoods_task.output,
                    'bucket' : bucket
                    },
    provide_context = True,
    dag=sf311_upload_dag,
)

# Define Glue crawler operator
glue_crawler_operator = PythonOperator(
    task_id='run_glue_crawler',
    python_callable=trigger_glue_crawler,
     op_kwargs={'aws_local_conn_id' : aws_conn_id_local,
                    'crawler_name':crawler_name,
                    'region': region
                    },
    dag=sf311_upload_dag,
)

# Define Glue crawler operator
# glue_crawler_operator = GlueCrawlerHook(
#     task_id='run_glue_crawler',
#     aws_conn_id=aws_conn_id_local,  # AWS connection ID configured in Airflow
#     crawler_name=crawler_name,  # Name of your Glue crawler
#     dag=sf311_upload_dag,
# )

#Set the task dependencies
s3_extract_last_task >> sf311_extract_task >> sf311_clean_task >> sf311_add_coord_task >> sf311_add_neighborhoods_task >> sf311_s3_upload_task >> glue_crawler_operator