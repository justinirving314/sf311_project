import json
import datetime
from datetime import datetime, timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import sf311_extract
import sf311_preprocess_1
import sf311_preprocess_2
import sf311_s3_upload

SOURCE_FILE_PATH = '/opt/airflow/dags/files'

sf311_upload_dag = DAG(
    dag_id='sf_data_upload',
    start_date=airflow.utils.dates.days_ago(1),
    tags=["data_pipeline"]
)


# Define the extract_data task
extract_task = PythonOperator(
    task_id='sf311_extract',
    python_callable=sf311_extract,
    dag=sf311_upload_dag,
)

clean_task = PythonOperator(
    task_id='sf311_clean',
    python_callable=sf311_preprocess_1,
    dag=sf311_upload_dag,
)

add_missing_coord_task = PythonOperator(
    task_id='sf311_add_coord',
    python_callable=sf311_preprocess_2,
    dag=sf311_upload_dag,
)

upload_task = PythonOperator(
    task_id='sf311_s3_upload',
    python_callable=sf311_s3_upload,
    dag=sf311_upload_dag,
)

#Set the task dependencies

extract_task >> clean_task >> add_missing_coord_task >> upload_task