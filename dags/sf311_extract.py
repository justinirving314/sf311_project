def pull_opensf_data(site, endpoint, app_token, api_key_id, api_secret_key):
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    # client = Socrata("data.sfgov.org", None)

    from sodapy import Socrata
    site = 'data.sfgov.org'
    app_token = 'uMKWWrv4GsbmHnifvkVHLSUfc'
    api_key_id = '26qt6l70lxcmk2wurqwc5ppjq'
    api_secret_key = '4n5ocishb77ot80qswzddsfzy26di1g0dnago4f2ynajjeay58'
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