def read_all_bucket_hook(access_key, access_secret_key, region, bucket, key):
    import boto3
    import io
    import pandas as pd
    from airflow.hooks.S3_hook import S3Hook
    from datetime import datetime
    import pyarrow.parquet as pq


    """
    Description: 
        The purpose of this function is to check all parquet files in the S3 bucket and determine the most
        recent date. This will then be used to limit the date range pulled from the SFData API to speed up the query
        and avoid duplication of results in our data warehouse.
    Inputs: 
        bucket: S3 bucket name where parquet files are stored
        key: S3 bucket subfolder where parquet files are stored

    Outputs:
        max_date: a datetime string
    
    
    """
     # Initialize the S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Assumes you have configured an AWS connection in Airflow
    
    # List all CSV files in the bucket/key
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=key)
    
    # Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()
    
    # Read and combine the content of each CSV file
    for key_test in keys:
        if key_test.endswith('.parquet'):
            print(key_test)
            # Read Parquet file from S3
            parquet_object = s3_hook.get_key(key=key_test, bucket_name=bucket)
            parquet_data = parquet_object.get()['Body'].read()
            # Parse Parquet data
            parquet_stream = io.BytesIO(parquet_data)
            parquet_file = pq.ParquetFile(parquet_stream)
            df = parquet_file.read().to_pandas()
            # pq_file = io.BytesIO(file_content)
            # df = pq.read_table(pq_file).to_pandas()
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        else:
            continue
    
    try:
        max_date = pd.to_datetime(combined_df['requested_datetime'].drop_duplicates().max())
        max_date = max_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    except:
        max_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    return max_date