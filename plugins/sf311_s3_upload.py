def upload_311_s3(df, 
                  bucket, 
                  aws_access_key, 
                  aws_access_secret_key):

    from s3_csv import open_s3
    from s3_csv import upload_to_s3
    from datetime import datetime
    import boto3
    import io
    import pandas as pd

    #Upload results to S3 bucket with naming convention of current datetime + raw_311_data
    int_311_key = f'int_311_data/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv' #export 
    s3_client = open_s3(aws_access_key, aws_access_secret_key)
    upload_to_s3(s3_client, df, bucket, int_311_key)

def upload_to_s3_new(df, 
                  bucket, 
                  aws_access_key, 
                  aws_secret_key):
    from datetime import datetime
    import boto3
    import io
    import pandas as pd
    import pyarrow as pa

    #Upload results to S3 bucket with naming convention of current datetime + raw_311_data
    int_311_key = f'int_311_data/{datetime.now():%Y-%m-%d_%H-%M-%S}.parquet' #export 

    s3_client = boto3.client("s3", 
                  aws_access_key_id = aws_access_key, 
                  aws_secret_access_key = aws_secret_key,
                  verify=False)
    
    # csv_buffer = io.StringIO()
    # df.to_csv(csv_buffer, header=True, index=False)
    # csv_buffer.seek(0)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key = int_311_key, Body = parquet_buffer.getvalue())