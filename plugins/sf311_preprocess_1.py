#Modify data types of certain columns prior to uploading
def raw_311_preprocess(df):
    import pandas as pd
    raw_311_results = df #xcom_pull is used to pull data from another task
    raw_311_results['service_request_id'] = raw_311_results['service_request_id'].astype(int)
    raw_311_results['requested_datetime'] = pd.to_datetime(raw_311_results['requested_datetime'])
    raw_311_results['updated_datetime'] = pd.to_datetime(raw_311_results['updated_datetime'])
    raw_311_results['lat'] = raw_311_results['lat'].astype(float)
    raw_311_results['long'] = raw_311_results['long'].astype(float)
    
    # Specify the character you want to filter columns by
    character_to_filter = '@'

    # Drop columns containing the specified character
    filtered_columns = raw_311_results.filter(like=character_to_filter, axis=1)
    int_311_results = raw_311_results.drop(columns=filtered_columns.columns)
    
    return int_311_results

