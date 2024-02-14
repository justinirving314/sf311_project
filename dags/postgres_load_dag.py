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
pg_hostname = os.getenv('POSTGRES_HOST')
pg_userid = os.getenv('AIRFLOW_USER')
pg_pw = os.getenv('AIRFLOW_PW')

#Import other libraries used in main DAG code
import json
import datetime
from datetime import datetime, timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import BaseOperator
from airflow.models import TaskInstance
from airflow.hooks.S3_hook import S3Hook
import pickle
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import base64
import boto3
from sqlalchemy import create_engine, text
from postgres_upload import pull_glue_table

SOURCE_FILE_PATH = '/opt/airflow/dags/files'

db_name = 'sf311_data'
table_name = 'sfdata_311'
aws_conn_id_local = 'aws_default' #this is the name of the AWS connection configured in your airflow instance that is called in S3 hooks
s3_output_path = 's3://jirving-sf311/athena_query_output/'
glue_table = 'int_311_data'
glue_db_name = 'sfdata311_s3_glue_database'

def pull_latest_date(pg_user, pg_pw, pg_host, db_name, table_name):
    # Set up the database connection URL
    db_url = f'postgresql://{pg_user}:{pg_pw}@{pg_host}:5432/{db_name}'

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    query = f"SELECT MAX(requested_datetime) FROM {table_name}"

    with engine.connect() as connection:
        result = connection.execute(text(query))
        row = result.fetchone()  # Retrieve the single row

    latest_datetime = pd.to_datetime(row[0]).strftime(format = '%Y-%m-%d %H:%M:%S')
    return latest_datetime


def upload_to_pg(pg_user, pg_pw, pg_host, db_name, result_df):
    # Set up the database connection URL
    db_url = f'postgresql://{pg_user}:{pg_pw}@{pg_host}:5432/{db_name}'

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # Assuming df is your Pandas DataFrame
    result_df.to_sql('sfdata_311', engine, if_exists='append', index=False)


# Define DAG instance with name and default run
postgres_upload_dag = DAG(
    dag_id='postgres_upload_dag',
    start_date=airflow.utils.dates.days_ago(1),
    tags=["postgres_upload"]
)

# Define task to pull latest date from existing postgres db
pg_pull_latest = PythonOperator(
    task_id='pg_pull_latest',
    python_callable=pull_latest_date,
    op_kwargs= {'pg_user': pg_userid, 
                     'pg_pw' : pg_pw, 
                     'pg_host' : pg_hostname,
                     'db_name' : db_name,
                     'table_name' : table_name},
    dag=postgres_upload_dag,
)

# Define task to pull latest data from glue catalogue database
glue_table_pull = PythonOperator(
    task_id='pull_glue_table',
    python_callable=pull_glue_table,
    op_kwargs= {'aws_local_conn_id': aws_conn_id_local, 
                     'glue_database' : glue_db_name, 
                     'glue_table' : glue_table,
                     's3_output_path' : s3_output_path,
                     'max_date' : pg_pull_latest.output},
    dag=postgres_upload_dag,
)

# Define task to pull latest data from glue catalogue database
upload_pg = PythonOperator(
    task_id='upload_pg',
    python_callable=upload_to_pg,
    op_kwargs= {'pg_user': pg_userid, 
                     'pg_pw' : pg_pw, 
                     'pg_host' : pg_hostname,
                     'db_name' : db_name,
                     'result_df' : glue_table_pull.output},
    dag=postgres_upload_dag,
)

pg_pull_latest >> glue_table_pull >> upload_pg