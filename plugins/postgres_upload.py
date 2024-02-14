def pull_glue_table(aws_local_conn_id, glue_database, glue_table, s3_output_path, max_date)
    # Modified code for batch execution of entire table
    # Initialize Amazon Athena client
    import time
    import pandas as pd
    import boto3
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook

    # athena_client = boto3.client("athena", 
    #                 aws_access_key_id = aws_access_key, 
    #                 aws_secret_access_key = aws_access_secret_key,
    #                 region_name = region,
    #                 verify=False)

    athena_hook = AthenaHook(aws_conn_id=aws_local_conn_id,
                             output_location=s3_output_path)



    # Define SQL query to count total number of rows
    count_query = f"SELECT COUNT(*) FROM {glue_database}.{glue_table} WHERE requested_datetime >= {max_date};"

    # Get query execution ID
    query_execution_id = athena_hook.run_query(count_query)

    # Wait for the query execution to complete
    athena_hook.wait_for_query(query_execution_id)
    
    row_count = athena_hook.get_query_results(query_execution_id)
    
    # query_execution = athena_client.start_query_execution(
    #     QueryString=count_query,
    #     QueryExecutionContext={'Database': glue_db_name},  # Specify your Glue database
    #     ResultConfiguration={'OutputLocation': s3_output},  # Specify S3 bucket for query results
    # )



    # Poll for the status of the query execution
    # while True:
    #     query_status = athena_hook.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
    #     if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
    #         break
    #     time.sleep(1)  # Wait for 1 second before polling again


    # Get total number of rows from query results
    # query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    total_rows = int(row_count['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])

    # Set batch size for pagination
    batch_size = 1000

    # Initialize list to store batch data
    all_data = []

    # Loop through batches
    for offset in range(0, total_rows, batch_size):
        # Define SQL query with LIMIT and OFFSET
        query = f"""
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER() AS row_num
                FROM {glue_database}.{glue_table}
            ) AS t
            WHERE t.row_num BETWEEN {offset + 1} AND {min(offset + batch_size, total_rows)}
            AND requested_datetime >= {max_date};
        """
        
        # Get query execution ID
        query_execution_id = athena_hook.run_query(query)

        # Wait for the query execution to complete
        athena_hook.wait_for_query(query_execution_id)

        # Execute query
        # query_execution = athena_client.start_query_execution(
        #     QueryString=query,
        #     QueryExecutionContext={'Database': glue_db_name},  # Specify your Glue database
        #     ResultConfiguration={'OutputLocation': s3_output},  # Specify S3 bucket for query results
        # )
        
        # Get query execution ID
        # query_execution_id = query_execution['QueryExecutionId']
        
        # Poll for the status of the query execution
        # while True:
        #     query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        #     if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        #         break
        #     time.sleep(1)  # Wait for 1 second before polling again
        
        # Get query results
        query_results = athena_hook.get_query_results(query_execution_id)
        
        column_names = []
        column_names = [col['Label'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        
        rows = []
        
        for row in query_results['ResultSet']['Rows'][1:]:
            # Handle different data types (e.g., VarChar, Integer, etc.)
            row_data = []
            for data in row['Data']:
                if 'VarCharValue' in data:
                    row_data.append(data['VarCharValue'])
                elif 'BigIntValue' in data:
                    row_data.append(int(data['BigIntValue']))
                # Add conditions for other data types as needed
                else:
                    row_data.append(None)  # Handle unsupported data types
            rows.append(row_data)
        
        # Convert data to DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        # Append batch data to list
        all_data.append(df)

    # Concatenate data from all batches into a single DataFrame
    result_df = pd.concat(all_data, ignore_index=True)

    return result_df