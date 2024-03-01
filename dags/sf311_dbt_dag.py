import os
import sys
# from cosmos import DbtRunOperator
# Get the current working directory
cwd = os.getcwd()
# Navigate up one level in the directory hierarchy
parent_dir = os.path.abspath(os.path.join(cwd, '..'))
working_dir = os.getcwd()
relative_path = os.path.join(parent_dir+'/plugins/')
# Add plugins folder for access to functions
sys.path.append(relative_path)
sys.path.append(parent_dir)


#Credentials loaded from .env file, new users of this notebook must create their own credentials and .env file
#after cloning the gitrepo
from dotenv import load_dotenv
load_dotenv()
pg_hostname = os.getenv('POSTGRES_HOST')
pg_userid = os.getenv('AIRFLOW_USER')
pg_pw = os.getenv('AIRFLOW_PW')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')

#Import other libraries used in main DAG code
import json
import datetime
from datetime import datetime, timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
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

SOURCE_FILE_PATH = '/opt/airflow/dags/files'
PATH_TO_DBT_PROJECT = '/opt/airflow/dags/dbt/dbt_311'
PATH_TO_DBT_VENV = '/opt/dbt_env/bin/activate'
print_dir = "ls"
dbt_command_staging = "source $PATH_TO_DBT_VENV && dbt run --models stg_sf311"
dbt_command_marts = "source $PATH_TO_DBT_VENV && dbt run --models daily_by_geo daily_total"
# Create the DAG with the specified schedule interval
dbt_daily_dag = DAG(
        dag_id = 'dbt_daily_dag', 
        schedule_interval=timedelta(days=1),
        start_date=airflow.utils.dates.days_ago(1),
        tags=["dbt_pipeline"])

# Define dbt tasks using BashOperator
dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command=dbt_command_staging,
    env={"PATH_TO_DBT_VENV":PATH_TO_DBT_VENV},
    cwd = PATH_TO_DBT_PROJECT,
    dag=dbt_daily_dag
)
dbt_mart = BashOperator(
    task_id='dbt_mart',
    bash_command=dbt_command_marts,
    env={"PATH_TO_DBT_VENV":PATH_TO_DBT_VENV},
    cwd = PATH_TO_DBT_PROJECT,
    dag=dbt_daily_dag
)
# Set task dependencies

# dbt_staging = DbtRunOperator(
#     task_id='dbt_staging',
#     dbt_command='run',  # Specify the dbt command to run
#     dbt_environment='production',  # Specify the dbt environment
#     dbt_profiles_dir='/dbt_311/dbt_311/',  # Specify the path to the dbt profiles directory
#     models = ['stg_sf311'],
#     dag=dbt_daily_dag,
# )

# dbt_mart = DbtRunOperator(
#     task_id='dbt_mart',
#     dbt_command='run',  # Specify the dbt command to run
#     dbt_environment='production',  # Specify the dbt environment
#     dbt_profiles_dir='/dbt_311/dbt_311/',  # Specify the path to the dbt profiles directory
#     models = ['daily_by_geo','daily_total','weekly_by_geo'],
#     dag=dbt_daily_dag,
# )

dbt_staging >> dbt_mart