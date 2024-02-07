import os
import sys
# Get the current working directory
cwd = os.getcwd()
# Navigate up one level in the directory hierarchy
parent_dir = os.path.abspath(os.path.join(cwd, '..'))
relative_path = os.path.join(parent_dir+'/plugins/')
sys.path.append(relative_path)

from dotenv import load_dotenv

#Credentials loaded from .env file, new users of this notebook must create their own credentials and .env file
#after cloning the gitrepo
load_dotenv()
app_token = os.getenv('APP_TOKEN')
api_key_id = os.getenv('API_KEY_ID')
api_secret_key = os.getenv('API_SECRET_KEY')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')

bucket = 'jirving-sf311' #os.getenv('S3_BUCKET')
site = 'data.sfgov.org'
endpoint = 'vw6y-z8j6'
geo_endpoint = 'sevw-6tgi'
df_spatial_txt = 'the_geom'

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
import pickle
import pandas as pd



SOURCE_FILE_PATH = '/opt/airflow/dags/files'

extract_filter = 'opened >= "2024-01-30"'

def upload_to_s3_hook(df, bucket):
    # import pandas as pd
    # from airflow.hooks.S3_hook import S3Hook
    # task_instance = kwargs['ti']
    # df_new = task_instance.xcom_pull(task_ids='sf311_add_neighborhoods_task')
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Assumes you have configured an AWS connection in Airflow
    int_311_key = f'int_311_data/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv' #export 
    csv_string = df.to_csv(index=False)
    s3_hook.load_string(string_data=csv_string, key=int_311_key, bucket_name=bucket)



sf311_upload_dag = DAG(
    dag_id='sf_data_upload',
    start_date=airflow.utils.dates.days_ago(1),
    tags=["data_pipeline"]
)


# Define the extract_data task
sf311_extract_task = PythonOperator(
    task_id='sf311_extract_task',
    python_callable=pull_opensf_data,
    op_kwargs= {'site': site, 
                     'endpoint' : endpoint, 
                     'app_token' : app_token,
                     'api_key_id' : api_key_id,
                     'api_secret_key' : api_secret_key,
                     'pulltype':'test'},
    dag=sf311_upload_dag,
)

sf311_clean_task = PythonOperator(
    task_id='sf311_clean_task',
    python_callable=raw_311_preprocess,
    op_kwargs={'df' : sf311_extract_task.output},
    dag=sf311_upload_dag,
)

sf311_add_coord_task = PythonOperator(
    task_id='sf311_add_coord_task',
    python_callable=address_to_coordinates,
    op_kwargs={'df' : sf311_clean_task.output},
    dag=sf311_upload_dag,
)

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

sf311_s3_upload_task = PythonOperator(
    task_id='sf311_s3_upload_task',
    python_callable=upload_to_s3_hook,
    op_kwargs={'df' : sf311_add_neighborhoods_task.output,
                    'bucket' : bucket
                    },
    provide_context = True,
    dag=sf311_upload_dag,
)

#Set the task dependencies
sf311_extract_task >> sf311_clean_task >> sf311_add_coord_task >> sf311_add_neighborhoods_task >> sf311_s3_upload_task