

def open_s3(access_key, access_secret_key, region):
    import boto3
    import io
    import pandas as pd
    s3 = boto3.client("s3", 
                  aws_access_key_id = access_key, 
                  aws_secret_access_key = access_secret_key,
                  region_name = region,
                  use_ssl=False)
    return s3

def read_s3(s3_client, bucket, key):
    import boto3
    import io
    import pandas as pd
    s3_file = s3_client.get_object(Bucket = bucket, Key = key)
    df = pd.read_csv(io.StringIO(s3_file['Body'].read().decode('utf-8')))
    return df

def read_all_bucket(access_key, access_secret_key, region, bucket, key):
    import boto3
    import io
    import pandas as pd
    from sf311_extract_date import open_s3
    from sf311_extract_date import read_s3

    s3_client = open_s3(access_key, access_secret_key, region)
    objects = s3_client.list_objects(Bucket=bucket, Prefix = key)
    df_comb = pd.DataFrame()
    # Iterate over the objects and read them
    for object_1 in objects['Contents']:
        key_test = object_1['Key']
        if key_test.endswith('.csv'):
            df = read_s3(s3_client, bucket, key_test)
            df_comb = pd.concat([df_comb,df], axis=1)
        else:
            continue
    
    try:
        max_date = pd.to_datetime(df_comb['requested_datetime'].drop_duplicates().max())
        max_date = max_date.strftime('%Y-%m-%dT%H:%M:%S')
    except:
        max_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return max_date