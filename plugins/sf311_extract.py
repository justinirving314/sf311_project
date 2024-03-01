def pull_opensf_data(site, 
                     endpoint, 
                     app_token, 
                     api_key_id, 
                     api_secret_key,
                     pulltype,
                     filters = None):
    
    import pandas as pd
    from sodapy import Socrata

    """
    Description:
        The purpose of this function is to pull all available 311 data from the SFData API using the date
        filter passed by the extractn date function.

    Inputs: 
        site: sf data website
        app_token: app_token created during set-up. 
        api_key_id: api key id created during set-up
        api_secret_key: secret api key created during set-up

    Outputs:
        all_records: a pandas dataframe of all records returned by the API pulls

    """


    #set limit and offset and create empty dataframe
    all_records = pd.DataFrame()
    limit = 40000
    offset = 0

    # Example authenticated client (needed for non-public datasets):
    client = Socrata(site,
                     app_token,
                     username=api_key_id,
                     password=api_secret_key)

    while True:
        results = client.get(endpoint, limit = limit, offset = offset, where=filters)
        
        #If there are no results leave the loop
        if not results:
            break

        #If results exist convert to dataframe and concat with existing running total dataframe
        results_df = pd.DataFrame.from_records(results)
        all_records = pd.concat([all_records, results_df], ignore_index=True)

        #Added an input in case we just want one sample of data instead of whole set
        if pulltype == 'test':
            break

        #Add the limit amount to the offset to keep cycling through the data
        offset += limit


    # Convert to pandas DataFrame
    
    
    return all_records
