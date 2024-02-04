def open_s3(access_key, access_secret_key):
    import boto3
    import io
    import pandas as pd
    s3 = boto3.client("s3", 
                  aws_access_key_id = access_key, 
                  aws_secret_access_key = access_secret_key,
                  verify=False)
    return s3

def upload_to_s3(s3_client, df, bucket, key):
    import boto3
    import io
    import pandas as pd
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key = key, Body = csv_buffer.getvalue())
    
def read_s3(s3_client, bucket, key):
    import boto3
    import io
    import pandas as pd
    s3_file = s3_client.get_object(Bucket = bucket, Key = key)
    df = pd.read_csv(io.StringIO(s3_file['Body'].read().decode('utf-8')))
    return df

def read_all_bucket(s3_client, bucket, key):
    
    objects = s3_client.list_objects(Bucket=bucket, Prefix = key)
    df_comb = pd.DataFrame()
    # Iterate over the objects and read them
    for object in objects['Contents']:
        key_test = object['Key']
        df = read_s3(s3_client, bucket, key_test)
        df_comb = pd.concat([df_comb,df], axis=1)
    return df_comb