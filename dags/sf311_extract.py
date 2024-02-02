def pull_opensf_data(site, endpoint, app_token, api_key_id, api_secret_key):
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    # client = Socrata("data.sfgov.org", None)

    from sodapy import Socrata
    site = 'data.sfgov.org'

    # Example authenticated client (needed for non-public datasets):
    client = Socrata(site,
                     app_token,
                     username=api_key_id,
                     password=api_secret_key)

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get(endpoint)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)
    
    return results_df